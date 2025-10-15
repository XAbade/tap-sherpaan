"""Pagination utilities for tap-sherpa."""

from enum import Enum
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, Generator
import time
from tenacity import retry, stop_after_attempt, wait_exponential

from singer_sdk import typing as th
from tap_sherpaan.streams import SherpaStream
from tap_sherpaan.client import SherpaClient


class PaginationMode(str, Enum):
    """Supported pagination modes."""

    TOKEN = "token"
    CURSOR = "cursor"
    OFFSET = "offset"


class PaginationConfig:
    """Configuration for pagination."""

    def __init__(
        self,
        chunk_size: int = 200,
        max_retries: int = 3,
        retry_wait_min: int = 4,
        retry_wait_max: int = 10,
        mode: PaginationMode = PaginationMode.TOKEN,
    ):
        """Initialize pagination config.

        Args:
            chunk_size: Number of records to process in each chunk
            max_retries: Maximum number of retry attempts
            retry_wait_min: Minimum wait time between retries in seconds
            retry_wait_max: Maximum wait time between retries in seconds
            mode: Pagination mode to use (TOKEN, CURSOR, or OFFSET)
        """
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self.retry_wait_min = retry_wait_min
        self.retry_wait_max = retry_wait_max
        self.mode = mode


class PaginatedStream(SherpaStream):
    """Base class for streams with pagination support."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the stream.

        Args:
            *args: Positional arguments to pass to parent class
            **kwargs: Keyword arguments to pass to parent class
        """
        super().__init__(*args, **kwargs)
        self._total_records = 0
        self._pagination_config = PaginationConfig(
            chunk_size=self.config.get("chunk_size", 200),
            max_retries=self.config.get("max_retries", 3),
            retry_wait_min=self.config.get("retry_wait_min", 4),
            retry_wait_max=self.config.get("retry_wait_max", 10),
            mode=PaginationMode.TOKEN,  # All streams use token-based pagination
        )
        # Initialize SherpaClient for SOAP requests
        self.client = SherpaClient(
            shop_id=self.config["shop_id"],
            tap=self._tap,
        )

    def map_record(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Pass through the API response directly without field mapping.

        Args:
            item: The response item to map

        Returns:
            The record as-is from the API
        """
        return item
    
    def _process_nested_objects(self, item: dict) -> dict:
        """Dynamically convert complex nested objects to JSON strings.
        
        Args:
            item: The raw item from the API response
            
        Returns:
            Processed item with nested objects converted to JSON strings
        """
        import json
        
        def clean_xml_artifacts(obj):
            """Recursively clean XML artifacts from the object."""
            if isinstance(obj, dict):
                # If it's an empty dict, return null
                if not obj:
                    return None
                cleaned = {}
                for key, value in obj.items():
                    # Skip XML namespace attributes
                    if key.startswith('@'):
                        continue
                    cleaned_value = clean_xml_artifacts(value)
                    # Only add non-null values to avoid empty dicts
                    if cleaned_value is not None:
                        cleaned[key] = cleaned_value
                # If all values were null, return null
                if not cleaned:
                    return None
                return cleaned
            elif isinstance(obj, list):
                return [clean_xml_artifacts(item) for item in obj]
            else:
                return obj
        
        def flatten_dict(d, parent_key='', sep='_'):
            """Recursively flatten nested dictionaries."""
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    # Always convert nested objects to JSON strings
                    items.append((new_key, json.dumps(clean_xml_artifacts(v))))
                elif isinstance(v, list):
                    # Convert lists to JSON strings
                    items.append((new_key, json.dumps(clean_xml_artifacts(v))))
                else:
                    items.append((new_key, v))
            return dict(items)
        
        # Start with the top-level fields, then dynamically process all nested data
        processed = {}
        
        # Process all fields in the item dynamically
        for key, value in item.items():
            if isinstance(value, dict):
                # Special handling for ItemInformation - extract fields from within it
                if key == "ItemInformation":
                    # Extract fields from ItemInformation and flatten them
                    for sub_key, sub_value in value.items():
                        if isinstance(sub_value, dict):
                            processed[sub_key] = json.dumps(clean_xml_artifacts(sub_value))
                        elif isinstance(sub_value, list):
                            processed[sub_key] = json.dumps(clean_xml_artifacts(sub_value))
                        else:
                            processed[sub_key] = sub_value
                else:
                    # Convert other nested objects to JSON strings
                    processed[key] = json.dumps(clean_xml_artifacts(value))
            elif isinstance(value, list):
                # Convert lists to JSON strings
                processed[key] = json.dumps(clean_xml_artifacts(value))
            else:
                # Simple values go directly
                processed[key] = value
        
        return processed

    def _get_state(self) -> Dict[str, Any]:
        """Get the current state with additional metadata.

        Returns:
            Dictionary containing the current state
        """
        state = super()._get_state()
        state.update({
            "last_sync": time.time(),
            "total_records_processed": self._total_records,
            "replication_key_value": self.get_starting_replication_key_value(None),
        })
        return state

    def _increment_stream_state(self, token: Union[str, Dict[str, Any]], context: Optional[Dict[str, Any]] = None) -> None:
        """Increment stream state with additional tracking.

        Args:
            token: The new token value or record dictionary
            context: Optional context dictionary
        """
        # Handle both dictionary and simple token values
        if isinstance(token, dict):
            token_value = token.get("token", token.get("Token", 0))
        else:
            token_value = token
            
        # Convert token to integer for consistent handling
        token_value = int(token_value)
        
        # Always pass a record with the replication key and value
        replication_key = getattr(self, "replication_key", "token")
        record = {replication_key: token_value}
        super()._increment_stream_state(record, context=context)
        self._total_records += 1

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _make_request(self, service_name: str, stream_name: str = None, **params: Any) -> Dict[str, Any]:
        """Make a request with retry logic.

        Args:
            service_name: Name of the service to call
            stream_name: Name of the stream (for stream-specific token)
            **params: Parameters to pass to the service

        Returns:
            Response from the service

        Raises:
            Exception: If the request fails after all retries
        """
        try:
            return self.client.call_service(service_name, stream_name=stream_name, **params)
        except Exception as e:
            self.logger.error(f"Error making request: {str(e)}")
            raise

    def get_records_with_pagination(
        self,
        service_name: str,
        context: Optional[dict] = None,
        **service_params: Any,
    ) -> Iterable[Dict[str, Any]]:
        """Get records using the configured pagination mode.

        Args:
            service_name: Name of the service to call
            **service_params: Additional parameters for the service call

        Yields:
            Dictionary objects representing records from the service
        """
        if self._pagination_config.mode == PaginationMode.TOKEN:
            yield from self.get_records_with_token(
                service_name=service_name,
                token_param_name="token",
                response_path=self.response_path,
                context=context,
                **service_params,
            )
        elif self._pagination_config.mode == PaginationMode.CURSOR:
            yield from self.get_records_with_cursor(
                service_name=service_name,
                context=context,
                **service_params,
            )
        else:  # OFFSET
            yield from self.get_records_with_offset(
                service_name=service_name,
                context=context,
                **service_params,
            )

    def get_records_with_token(
        self,
        service_name: str,
        token_param_name: str,
        response_path: str,
        context: Optional[dict] = None,
        **call_params,
    ) -> Generator[Dict[str, Any], None, None]:
        """Get records using token-based pagination.

        Args:
            service_name: Name of the SOAP service to call
            token_param_name: Name of the token parameter in the SOAP call
            response_path: Path to the response items in the response dict (dot-separated string)
            **call_params: Additional parameters to pass to the SOAP call

        Yields:
            Records from the API
        """
        # 1. Get initial token from state bookmarks
        last_token = self.get_starting_replication_key_value(context)
        if last_token is None:
            last_token = 1

        self.logger.info(f"[{self.name}] Starting sync with token: {last_token}")

        while True:
            # 2. Make request using current token
            call_params[token_param_name] = last_token
            response = self._make_request(service_name, stream_name=self.name, **call_params)
            response_time = response.get("ResponseTime", 0)

            # 3. Extract items from response
            items = response
            for part in response_path.split('.'):
                if isinstance(items, dict):
                    items = items.get(part, {})
                else:
                    items = {}
            
            if not items or items == {}:
                self.logger.info(f"[{self.name}] Empty response, stopping pagination")
                break

            # Ensure items is a list
            if not isinstance(items, list):
                items = []

            # 4. Process items and find highest token
            highest_token = int(last_token)
            for item in items:
                # Get token before mapping record
                item_token = int(item.get("Token", 0))
                if item_token > highest_token:
                    highest_token = item_token

                # Map and yield record
                record = self.map_record(item)
                if record:
                    record["response_time"] = response_time
                    yield record

            # 5. Update token for next request
            if highest_token > 0:
                # Since API always returns tokens > request token, we can use highest_token directly
                next_token = str(highest_token)
                self.logger.info(f"[{self.name}] Token progression: {last_token} -> {next_token} (batch size: {len(items)})")
                last_token = next_token
                self._increment_stream_state(last_token)
                self._write_state_message()
            else:
                self.logger.info(f"[{self.name}] No valid tokens found in response, stopping pagination")
                break

    def get_records_with_cursor(
        self,
        service_name: str,
        context: Optional[dict] = None,
        cursor_param_name: str = "cursor",
        **service_params: Any,
    ) -> Iterable[Dict[str, Any]]:
        """Get records using cursor-based pagination.

        Args:
            service_name: Name of the service to call
            cursor_param_name: Name of the cursor parameter in the service call
            **service_params: Additional parameters for the service call

        Yields:
            Dictionary objects representing records from the service
        """
        cursor = self.get_starting_replication_key_value(context)
        
        while True:
            # Make the request with retry logic
            response = self._make_request(
                service_name,
                **{cursor_param_name: cursor, **service_params}
            )
            
            # Process records
            records = response.get("records", [])
            if not records:
                break
                
            for record in records:
                yield record
                cursor = record.get("cursor")
            
            # Update state
            self._increment_stream_state(cursor)
            self._write_state_message()

    def get_records_with_offset(
        self,
        service_name: str,
        context: Optional[dict] = None,
        offset_param_name: str = "offset",
        limit_param_name: str = "limit",
        **service_params: Any,
    ) -> Iterable[Dict[str, Any]]:
        """Get records using offset-based pagination.

        Args:
            service_name: Name of the service to call
            offset_param_name: Name of the offset parameter in the service call
            limit_param_name: Name of the limit parameter in the service call
            **service_params: Additional parameters for the service call

        Yields:
            Dictionary objects representing records from the service
        """
        offset = 0
        limit = self._pagination_config.chunk_size
        
        while True:
            # Make the request with retry logic
            response = self._make_request(
                service_name,
                **{
                    offset_param_name: offset,
                    limit_param_name: limit,
                    **service_params
                }
            )
            
            # Process records
            records = response.get("records", [])
            if not records:
                break
                
            for record in records:
                yield record
            
            # Update offset
            offset += len(records)
            self._increment_stream_state(str(offset))
            self._write_state_message()
            
            # If we got fewer records than the limit, we're done
            if len(records) < limit:
                break 

    def get_starting_replication_key_value(self, context: Optional[dict] = None) -> Optional[str]:
        """Get the starting replication key value from state only. Config is not a source of truth."""
        state = self._tap_state
        if "bookmarks" in state and self.name in state["bookmarks"]:
            return state["bookmarks"][self.name].get("replication_key_value")
        # Default to 1 if no token found in state
        return "1"


    def get_records_with_custom_client_method(
        self,
        client_method_name: str,
        **method_params: Any,
    ) -> Iterable[Dict[str, Any]]:
        """Generic method for streams using custom client methods with token-based pagination.

        Args:
            client_method_name: Name of the client method to call
            **method_params: Additional parameters to pass to the client method

        Yields:
            Dictionary objects representing records from the API
        """
        token = self.get_starting_replication_key_value()
        if token is None:
            token = "1"  # Default starting token
        
        # Derive result_key from client_method_name (e.g., "get_changed_stock" -> "ResponseValue")
        # The actual result is always in "ResponseValue" for these SOAP services
        result_key = "ResponseValue"
        
        # Extract items_key from response_path (e.g., "ItemInformation" -> "ItemInformation")
        # For ChangedItemsInformation, the actual items are in "ItemCodeTokenItemInformation"
        if self.name == "changed_items_information":
            items_key = "ItemCodeTokenItemInformation"
        elif self.name == "changed_stock":
            items_key = "ItemStockToken"
        elif self.name == "changed_suppliers":
            items_key = "ClientCodeToken"
        else:
            # Default: extract from response_path
            if self.response_path.startswith("ResponseValue."):
                items_key = self.response_path.split('.')[-1]
            else:
                items_key = self.response_path
        
        while True:
            # Check if this is a stream method (returns XML string) or client method
            if hasattr(self, client_method_name):
                # Stream method - get SOAP envelope and call generic client method
                envelope_method = getattr(self, client_method_name)
                soap_envelope = envelope_method(token=int(token), **method_params)
                # Call generic client method to send the envelope
                # Convert "get_changed_items_information" to "ChangedItemsInformation"
                service_name = client_method_name.replace("get_", "").replace("_", " ").title().replace(" ", "")
                response = self.client.call_custom_soap_service(
                    service_name=service_name,
                    soap_envelope=soap_envelope
                )
            else:
                # Regular client method
                client_method = getattr(self.client, client_method_name)
                response = client_method(token=int(token), **method_params)
            
            if not response:
                self.logger.info(f"[{self.name}] Empty response, stopping pagination")
                break
                
            # Handle raw response from custom SOAP service
            if "raw_response" in response:
                # Parse the raw XML response
                from lxml import etree
                try:
                    root = etree.fromstring(response["raw_response"].encode('utf-8'))
                except Exception as e:
                    self.logger.error(f"[{self.name}] Failed to parse XML response: {e}")
                    break
                
                # Find the result element
                result_element = root.find(f".//{{{self.client.client.wsdl.types.prefix_map.get('ns0', 'http://sherpa.sherpaan.nl/')}}}{result_key}")
                
                if result_element is None:
                    self.logger.info(f"[{self.name}] No {result_key} found in response, stopping pagination")
                    break
                
                # Convert XML to JSON using xmltodict for cleaner structure
                import xmltodict
                
                # Convert XML to dict using xmltodict
                xml_dict = xmltodict.parse(response["raw_response"])
                
                # Navigate to the actual data - this will work for all streams
                soap_body = xml_dict.get("soap:Envelope", {}).get("soap:Body", {})
                
                # Find the response data dynamically (works for all stream types)
                response_data = None
                for key, value in soap_body.items():
                    if "Response" in key and isinstance(value, dict):
                        result_key = key.replace("Response", "Result")
                        if result_key in value:
                            response_data = value[result_key]
                            self.logger.info(f"[{self.name}] Found response_data: {type(response_data)}")
                            if isinstance(response_data, dict):
                                self.logger.info(f"[{self.name}] response_data keys: {list(response_data.keys())}")
                            break
                
                if response_data:
                    response = response_data
                    # Debug: check what's in ResponseValue
                    if isinstance(response, dict) and "ResponseValue" in response:
                        self.logger.info(f"[{self.name}] ResponseValue content type: {type(response['ResponseValue'])}")
                        if isinstance(response['ResponseValue'], dict):
                            self.logger.info(f"[{self.name}] ResponseValue keys: {list(response['ResponseValue'].keys())}")
                        elif isinstance(response['ResponseValue'], list):
                            self.logger.info(f"[{self.name}] ResponseValue is list with {len(response['ResponseValue'])} items")
                        else:
                            self.logger.info(f"[{self.name}] ResponseValue content: {response['ResponseValue']}")
                else:
                    self.logger.error(f"[{self.name}] Could not find response data in XML")
                    break
            
            # After xmltodict conversion, we have ResponseValue containing the items
            # So we should use 'ResponseValue' as the result key
            if "ResponseValue" in response:
                result = response["ResponseValue"]
                self.logger.info(f"[{self.name}] Using ResponseValue as result, type: {type(result)}")
            else:
                self.logger.error(f"[{self.name}] No ResponseValue in response, stopping pagination")
                break
            if not result:
                self.logger.info(f"[{self.name}] No data in result, stopping pagination")
                break
                
            # Now result contains the ResponseValue dict with the items
            # Extract the items from the result
            if isinstance(result, dict):
                # For xmltodict, the items are in result[items_key]
                if items_key in result:
                    items = result[items_key]
                    if not isinstance(items, list):
                        items = [items]
                    self.logger.info(f"[{self.name}] Found {len(items)} items in '{items_key}'")
                else:
                    # Try to find the items in common patterns
                    possible_keys = ["ItemCodeTokenItemInformation", "ItemCodeTokenStock", "ItemCodeTokenSupplier", "ResponseValue"]
                    items = []
                    for key in possible_keys:
                        if key in result:
                            items = result[key]
                            if not isinstance(items, list):
                                items = [items]
                            self.logger.info(f"[{self.name}] Found {len(items)} items in '{key}'")
                            break
                    
                    if not items:
                        # Check if result itself is a single item (for streams like SupplierInfo)
                        if isinstance(result, dict) and any(key in result for key in ["SupplierCode", "ItemCode", "ClientCode"]):
                            items = [result]
                            self.logger.info(f"[{self.name}] Using result as single item")
                        else:
                            self.logger.error(f"[{self.name}] Could not find items in result")
                            break
            elif isinstance(result, list):
                # If result is already a list, use it directly
                items = result
                self.logger.info(f"[{self.name}] Using result as list with {len(items)} items")
            else:
                self.logger.error(f"[{self.name}] Unexpected result type: {type(result)}")
                break
            
            if not items:
                self.logger.info(f"[{self.name}] No items found, stopping pagination")
                break
                
            # Ensure items is a list
            if not isinstance(items, list):
                items = [items]
                
            # Process items and find highest token
            highest_token = int(token)
            # Get response_time from the original response if it's a dict
            if isinstance(response, dict) and "ResponseTime" in response:
                response_time = response["ResponseTime"]
            else:
                response_time = 0
            
            for item in items:
                # Get token before mapping record
                if isinstance(item, dict):
                    item_token = int(item.get("Token", 0))
                else:
                    self.logger.error(f"[{self.name}] Item is not a dict: {type(item)} - {item}")
                    continue
                if item_token > highest_token:
                    highest_token = item_token

                # Convert complex nested objects to JSON strings for array columns
                processed_item = self._process_nested_objects(item)
                
                # Map and yield record - automatic field mapping will handle everything
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

        Args:
            context: Stream context (ignored, kept for Singer SDK compatibility)

        Yields:
            Dictionary objects representing records from the API
        """
        token = self.get_starting_replication_key_value()
        service_params = {
            "securityCode": self.config["security_code"],
            "token": token,
        }
        # All streams use chunk_size from config
        if self.name in [
            "changed_orders",
            "changed_parcels", 
            "changed_purchases",
            "changed_suppliers",
            "changed_item_suppliers",
        ]:
            service_params["count"] = str(self._pagination_config.chunk_size)
        elif self.name == "changed_stock":
            service_params["maxResult"] = str(self._pagination_config.chunk_size)
        # For changed_items, do not add count or maxResult
        # Derive service name from stream name (e.g., "changed_suppliers" -> "ChangedSuppliers")
        service_name = ''.join(word.capitalize() for word in self.name.split('_'))
        
        yield from self.get_records_with_pagination(
            service_name=service_name,
            context=context,
            **service_params,
        )
