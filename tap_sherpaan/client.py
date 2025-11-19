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
        self.wsdl_url = f"https://sherpaservices-tst.sherpacloud.eu/{shop_id}/Sherpa.asmx?wsdl"
        #self.wsdl_url = f"https://sherpaservices-prd.sherpacloud.eu/{shop_id}/Sherpa.asmx?wsdl"
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
    
    # Default to pagination enabled
    paginate = True

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
        """Dynamically convert nested XML objects to flattened fields / JSON strings.

        This mirrors the behaviour of the previous tap implementation:
        - Simple fields under the ``General`` section are flattened to top-level keys
          (so they line up with the JSON schema fields like ``ItemType``, ``Description``).
        - Complex nested objects and lists are serialized to JSON strings.
        - XML artefacts and pure ``xsi:nil`` markers become ``None``.
        """

        def clean_xml_artifacts(obj):
            """Recursively clean XML artifacts from the object."""
            if isinstance(obj, dict):
                # If it's an empty dict, return null
                if not obj:
                    return None
                cleaned = {}
                for key, value in obj.items():
                    # Skip XML namespace/attribute keys (e.g. '@xsi:nil')
                    if key.startswith("@"):
                        continue
                    cleaned_value = clean_xml_artifacts(value)
                    # Only add non-null values to avoid empty dicts
                    if cleaned_value is not None:
                        cleaned[key] = cleaned_value
                # If all values were null or XML artefacts, return null
                if not cleaned:
                    return None
                return cleaned
            elif isinstance(obj, list):
                return [clean_xml_artifacts(v) for v in obj]
            else:
                return obj

        def flatten_dict(d: dict, parent_key: str = "", sep: str = "_") -> dict:
            """Recursively flatten nested dictionaries.

            - Fields under ``General`` are flattened without the ``General`` prefix.
            - Complex nested dicts and lists are converted to JSON strings.
            """
            items: list[tuple[str, Any]] = []
            for k, v in d.items():
                # Skip XML namespace attributes
                if k.startswith("@"):
                    continue

                # For General section, don't add prefix to match schema field names
                if parent_key == "General":
                    new_key = k
                else:
                    new_key = f"{parent_key}{sep}{k}" if parent_key else k

                if isinstance(v, dict):
                    # Detect dictionaries that only contain XML attributes (e.g. xsi:nil)
                    non_attr_keys = [key for key in v.keys() if not key.startswith("@")]
                    if not non_attr_keys:
                        # Pure xsi:nil-style field → None
                        items.append((new_key, None))
                    elif k == "General":
                        # Always fully flatten the General section
                        flattened = flatten_dict(v, new_key, sep)
                        items.extend(flattened.items())
                    else:
                        # Decide whether to flatten or JSON-encode this dict
                        has_nested_objects = any(
                            isinstance(val, (dict, list)) for val in v.values()
                        )
                        if has_nested_objects:
                            items.append((new_key, json.dumps(clean_xml_artifacts(v))))
                        else:
                            flattened = flatten_dict(v, new_key, sep)
                            items.extend(flattened.items())
                elif isinstance(v, list):
                    # Lists always become JSON strings
                    items.append((new_key, json.dumps(clean_xml_artifacts(v))))
                else:
                    items.append((new_key, v)
                    )
            return dict(items)

        # Start with the top-level fields, then dynamically process nested data
        processed: Dict[str, Any] = {}

        for key, value in item.items():
            if isinstance(value, dict):
                flattened = flatten_dict(value)
                processed.update(flattened)
            elif isinstance(value, list):
                processed[key] = json.dumps(clean_xml_artifacts(value))
            else:
                processed[key] = value

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
        # Check if pagination is disabled for this stream
        if not self.paginate:
            # Non-paginated stream - make single request with token=0
            token = "0"
            self.logger.info(f"[{self.name}] Making single request (no pagination)")
        else:
            # Paginated stream - get token from state
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

            # Update token for next request (only if pagination is enabled)
            if self.paginate:
                if highest_token > int(token):
                    next_token = str(highest_token)
                    self.logger.info(f"[{self.name}] Token progression: {token} -> {next_token} (batch size: {len(items)})")
                    token = next_token
                    self._increment_stream_state(token)
                    self._write_state_message()
                else:
                    self.logger.info(f"[{self.name}] No valid tokens found in response, stopping pagination")
                    break
            else:
                # Non-paginated stream - only one request, break after processing
                break

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        """Get records from the API.
        
        This method should be overridden by specific stream classes.
        """
        raise NotImplementedError("Stream classes must implement get_records")
