"""SOAP client and base stream class for tap-sherpaan."""

from __future__ import annotations

import html
import json
import logging
from typing import Any, Callable, Dict, Iterable, Optional, Union

import xmltodict
from requests import Session
from tenacity import retry, stop_after_attempt, wait_exponential
from zeep import Client, Settings
from zeep.transports import Transport

from singer_sdk.streams import Stream

# Set up logging
logging.basicConfig(level=logging.INFO)
logging.getLogger("zeep").setLevel(logging.WARNING)
logging.getLogger("zeep.transports").setLevel(logging.WARNING)
logging.getLogger("zeep.xsd.schema").setLevel(logging.WARNING)
logging.getLogger("zeep.wsdl.wsdl").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


class SherpaClient:
    """SOAP client for Sherpa API."""

    def __init__(
        self,
        shop_id: str,
        tap: "TapSherpaan",
        timeout: int = 300,
    ) -> None:
        """Initialize the Sherpa SOAP client.

        Args:
            shop_id: The shop ID for the Sherpa SOAP service
            tap: The tap instance to get configuration from
            timeout: Request timeout in seconds
        """
        self.shop_id = shop_id
        self.wsdl_url = f"https://sherpaservices-prd.sherpacloud.eu/{shop_id}/Sherpa.asmx?wsdl"
        session = Session()
        session.headers.update({
            "Content-Type": "text/xml; charset=utf-8",
            "User-Agent": "PostmanRuntime/7.32.3",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive"
        })
        transport = Transport(session=session, timeout=timeout)
        settings = Settings(strict=False)
        self.client = Client(
            self.wsdl_url,
            transport=transport,
            settings=settings,
        )
        self.tap = tap
        self.session = session

    def call_custom_soap_service(self, service_name: str, soap_envelope: str) -> dict:
        """Call a SOAP service with a custom envelope.

        Args:
            service_name: Name of the SOAP service (for SOAPAction header)
            soap_envelope: The complete SOAP envelope XML

        Returns:
            Response from the SOAP service
        """
        self.session.headers.update({
            "SOAPAction": f'"http://sherpa.sherpaan.nl/{service_name}"'
        })

        try:
            response = self.session.post(
                self.wsdl_url.replace("?wsdl", ""),
                data=soap_envelope,
                timeout=300
            )
            response.raise_for_status()
            return {"raw_response": response.text}
        except Exception as e:
            self.tap.logger.error(f"Error in call_custom_soap_service: {e}")
            raise


class SherpaStream(Stream):
    """Base stream class for Sherpa streams with pagination support."""

    def __init__(self, *args, **kwargs):
        """Initialize the stream."""
        super().__init__(*args, **kwargs)
        self.client = SherpaClient(
            shop_id=self.config["shop_id"],
            tap=self._tap,
        )
        self._total_records = 0

    def map_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Pass through the API response directly without field mapping."""
        return item

    def _process_nested_objects(self, item: dict) -> dict:
        """Convert complex nested objects to JSON strings."""
        def clean_xml_artifacts(obj):
            """Recursively clean XML artifacts from the object."""
            if isinstance(obj, dict):
                if not obj:
                    return None
                return {k: clean_xml_artifacts(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_xml_artifacts(v) for v in obj if v is not None]
            else:
                return obj

        processed = clean_xml_artifacts(item)
        
        # Convert nested dicts to JSON strings for complex fields
        for key, value in processed.items():
            if isinstance(value, (dict, list)) and value:
                processed[key] = json.dumps(value)
        
        return processed

    def get_starting_replication_key_value(self, context: Optional[dict] = None) -> Optional[str]:
        """Get the starting replication key value from state.
        
        Args:
            context: Optional stream context
            
        Returns:
            Starting token value as string, or "0" if not found
        """
        state = self._tap_state
        if "bookmarks" in state and self.name in state["bookmarks"]:
            bookmark = state["bookmarks"][self.name]
            replication_key = getattr(self, "replication_key", "Token")
            replication_key_value = bookmark.get(replication_key) or bookmark.get("replication_key_value")
            if replication_key_value is not None:
                return str(replication_key_value)
        return "0"

    def _increment_stream_state(self, token: Union[str, Dict[str, Any]], context: Optional[Dict[str, Any]] = None) -> None:
        """Increment stream state with token value.
        
        Args:
            token: The new token value
            context: Optional context dictionary
        """
        if isinstance(token, dict):
            token_value = token.get("Token", token.get("token", 0))
        else:
            token_value = token
        
        token_value = int(token_value)
        replication_key = getattr(self, "replication_key", "Token")
        record = {replication_key: token_value}
        super()._increment_stream_state(record, context=context)
        self._total_records += 1

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _make_soap_request(self, service_name: str, soap_envelope: str, token: Optional[int] = None) -> dict:
        """Make a SOAP request with retry logic.
        
        Args:
            service_name: Name of the SOAP service
            soap_envelope: SOAP envelope XML string
            token: Optional token value for logging
            
        Returns:
            Parsed response dictionary
        """
        try:
            response = self.client.call_custom_soap_service(service_name, soap_envelope)
            return self._parse_soap_response(response["raw_response"], service_name)
        except Exception as e:
            self.logger.error(f"[{self.name}] Error making SOAP request to {service_name}: {str(e)}")
            raise

    def _parse_soap_response(self, xml_response: str, service_name: str) -> dict:
        """Parse SOAP XML response to dictionary.
        
        Args:
            xml_response: Raw XML response string
            service_name: Name of the service (for logging)
            
        Returns:
            Parsed response dictionary
        """
        try:
            xml_dict = xmltodict.parse(xml_response)
            soap_body = xml_dict.get("soap:Envelope", {}).get("soap:Body", {})
            
            # Find the response data dynamically
            response_data = None
            for key, value in soap_body.items():
                if "Response" in key and isinstance(value, dict):
                    result_key = key.replace("Response", "Result")
                    if result_key in value:
                        response_data = value[result_key]
                        break
            
            if response_data:
                return response_data
            
            # Fallback: try to find ResponseValue directly
            for key, value in soap_body.items():
                if isinstance(value, dict) and "ResponseValue" in value:
                    return value["ResponseValue"]
            
            return {}
        except Exception as e:
            self.logger.error(f"Failed to parse SOAP response: {e}")
            return {}

    def get_records_with_token_pagination(
        self,
        get_soap_envelope: Callable[[int, int], str],
        service_name: str,
        items_key: str,
        context: Optional[dict] = None,
        page_size: int = 200,
    ) -> Iterable[Dict[str, Any]]:
        """Get records using token-based pagination.
        
        Args:
            get_soap_envelope: Function that generates SOAP envelope (token, count)
            service_name: Name of the SOAP service
            items_key: Key in response containing the items list
            context: Optional stream context
            page_size: Number of records per page
            
        Yields:
            Records from the API
        """
        token = self.get_starting_replication_key_value(context)
        if not token or token == "0":
            token = "1"
        
        self.logger.info(f"[{self.name}] Starting sync with token: {token}")

        while True:
            # Generate SOAP envelope
            current_token = int(token)
            soap_envelope = get_soap_envelope(token=current_token, count=page_size)
            
            # Make request with token logging
            self.logger.info(f"[{self.name}] Requesting {service_name} with token: {current_token}, page_size: {page_size}")
            response = self._make_soap_request(service_name, soap_envelope, token=current_token)
            
            # Extract items from response
            items = response.get(items_key, [])
            if not items:
                # Try ResponseValue as fallback
                if "ResponseValue" in response:
                    response_value = response["ResponseValue"]
                    if isinstance(response_value, dict) and items_key in response_value:
                        items = response_value[items_key]
                    elif isinstance(response_value, list):
                        items = response_value
                
            if not items:
                self.logger.info(f"[{self.name}] No data in result, stopping pagination")
                break
            
            # Ensure items is a list
            if not isinstance(items, list):
                items = [items]
            
            if not items:
                self.logger.info(f"[{self.name}] Empty response, stopping pagination")
                break
            
            self.logger.info(f"[{self.name}] Found {len(items)} items in '{items_key}'")
            
            # Process items and find highest token
            highest_token = int(token)
            response_time = response.get("ResponseTime", 0)
            
            for item in items:
                if not isinstance(item, dict):
                    continue
                    
                item_token = int(item.get("Token", 0))
                if item_token > highest_token:
                    highest_token = item_token

                # Process nested objects
                processed_item = self._process_nested_objects(item)
                
                # Map and yield record
                record = self.map_record(processed_item)
                if record:
                    record["response_time"] = response_time
                    yield record

            # Update token for next request
            if highest_token > int(token):
                next_token = str(highest_token)
                self.logger.info(f"[{self.name}] Token progression: {token} -> {next_token} (batch size: {len(items)})")
                token = next_token
                self._increment_stream_state(token)
                self._write_state_message()
            else:
                self.logger.info(f"[{self.name}] No valid tokens found in response, stopping pagination")
                break

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        """Get records from the API.
        
        This method should be overridden by specific stream classes.
        """
        raise NotImplementedError("Stream classes must implement get_records")
