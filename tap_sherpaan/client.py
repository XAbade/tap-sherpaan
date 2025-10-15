"""Custom client handling for Sherpa API."""
from __future__ import annotations

import typing as t
from zeep import Client, Settings
from zeep.transports import Transport
from requests import Session
from zeep.helpers import serialize_object
import logging

# Set up logging: only show warnings or above for zeep and its submodules
logging.basicConfig(level=logging.INFO)
logging.getLogger("zeep").setLevel(logging.WARNING)
logging.getLogger("zeep.transports").setLevel(logging.WARNING)
logging.getLogger("zeep.xsd.schema").setLevel(logging.WARNING)
logging.getLogger("zeep.wsdl.wsdl").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class SherpaClient:
    """SOAP client for Sherpa API."""

    def __init__(
        self,
        shop_id: str,
        tap: "TapSherpaan",
        timeout: int = 30,
    ) -> None:
        """Initialize the Sherpa SOAP client.

        Args:
            shop_id: The shop ID for the Sherpa SOAP service
            tap: The tap instance to get configuration from
            timeout: Request timeout in seconds
        """
        self.shop_id = shop_id
        self.wsdl_url = f"https://sherpaservices-tst.sherpacloud.eu/{shop_id}/Sherpa.asmx?wsdl"
        session = Session()
        # Set default headers (Postman-style)
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
        self.session = session  # Save session for dynamic header updates

    def get_curl_command(self, service_name: str, params: dict, stream_name: str = None) -> str:
        """Generate a curl command for the SOAP request.

        Args:
            service_name: Name of the SOAP service method to call
            params: Parameters to pass to the service method
            stream_name: Name of the stream (for stream-specific token)

        Returns:
            String containing the curl command
        """
        # Add authentication parameters
        params = {
            "securityCode": self.tap.config["security_code"],
            **params
        }

        # Convert all parameters to strings
        params = {k: str(v) for k, v in params.items()}

        # Create the SOAP envelope
        soap_envelope = f"""<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<soap:Envelope xmlns:soap=\"http://www.w3.org/2003/05/soap-envelope\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\n  <soap:Body>\n    <{service_name} xmlns=\"http://sherpa.sherpaan.nl/\">\n{chr(10).join(f'      <{k}>{v}</{k}>' for k, v in params.items())}\n    </{service_name}>\n  </soap:Body>\n</soap:Envelope>"""

        # Generate the curl command
        curl_cmd = f"""curl -X POST \\\n  '{self.wsdl_url.replace("?wsdl", "")}' \\\n  -H 'Content-Type: application/soap+xml; charset=utf-8' \\\n  -H 'SOAPAction: \"http://sherpa.sherpaan.nl/{service_name}\"' \\\n  -d '{soap_envelope}'"""

        return curl_cmd

    def call_service(self, service_name: str, stream_name: str = None, **kwargs) -> dict:
        """Call a SOAP service method.

        Args:
            service_name: Name of the SOAP service method to call
            stream_name: Name of the stream (for stream-specific token)
            **kwargs: Arguments to pass to the service method

        Returns:
            Response from the SOAP service
        """
        # Add authentication parameters to all requests
        kwargs["securityCode"] = self.tap.config["security_code"]

        # Ensure all parameters are strings
        kwargs = {k: str(v) for k, v in kwargs.items()}

        # Set SOAPAction header dynamically for this call
        soap_action = f'"http://sherpa.sherpaan.nl/{service_name}"'
        self.session.headers["SOAPAction"] = soap_action

        # Get the service and create the operation
        service = self.client.create_service(
            '{http://sherpa.sherpaan.nl/}SherpaServiceSoap12',
            'https://sherpaservices-tst.sherpacloud.eu/214/Sherpa.asmx'
        )
        
        # Call the service method (no _http_headers)
        method = getattr(service, service_name)
        result = method(**kwargs)
        return serialize_object(result)

    def call_custom_soap_service(self, service_name: str, soap_envelope: str) -> dict:
        """Generic method for custom SOAP requests.

        Args:
            service_name: Name of the SOAP service (for SOAPAction header)
            soap_envelope: The complete SOAP envelope XML

        Returns:
            Response from the SOAP service
        """
        # Update headers for this specific request
        self.session.headers.update({
            "SOAPAction": f'"http://sherpa.sherpaan.nl/{service_name}"'
        })

        try:
            # Make the request using the session directly
            response = self.session.post(
                self.wsdl_url.replace("?wsdl", ""),
                data=soap_envelope,
                timeout=30
            )
            response.raise_for_status()
            
            # Simple response parsing - just return the raw response
            # The pagination logic will handle the parsing
            return {"raw_response": response.text}
            
        except Exception as e:
            self.tap.logger.error(f"Error in call_custom_soap_service: {e}")
            raise

    def get_changed_orders_information(self, token: int, count: int = 200) -> dict:
        """Get changed orders information from the API with custom SOAP request.

        Args:
            token: The token to use for pagination
            count: Number of orders to fetch per request
        """
        # Create custom SOAP envelope with the specific format
        soap_envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:ChangedOrdersInformation xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.tap.config["security_code"]}</tns:securityCode>
      <tns:token>{token}</tns:token>
      <tns:count>{count}</tns:count>
      <tns:orderInformationTypes>
        <tns:OrderInformationType>General</tns:OrderInformationType>
        <tns:OrderInformationType>OrderLines</tns:OrderInformationType>
      </tns:orderInformationTypes>
    </tns:ChangedOrdersInformation>
  </soap12:Body>
</soap12:Envelope>"""

        return self.call_custom_soap_service("ChangedOrdersInformation", soap_envelope)

    def get_supplier_info(self, supplier_code: str) -> dict:
        """Get supplier information from the API.

        Args:
            supplier_code: The supplier code to get information for
        """
        # Create custom SOAP envelope with the specific format
        soap_envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:SupplierInfo xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.tap.config["security_code"]}</tns:securityCode>
      <tns:supplierCode>{supplier_code}</tns:supplierCode>
    </tns:SupplierInfo>
  </soap12:Body>
</soap12:Envelope>"""

        return self.call_custom_soap_service("SupplierInfo", soap_envelope)

    def get_purchase_info(self, purchase_number: str) -> dict:
        """Get purchase information from the API.

        Args:
            purchase_number: The purchase number to get information for

        Returns:
            Response from the PurchaseInfo service
        """
        # Create custom SOAP envelope with the specific format
        soap_envelope = f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:PurchaseInfo xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.tap.config["security_code"]}</tns:securityCode>
      <tns:purchaseNumber>{purchase_number}</tns:purchaseNumber>
    </tns:PurchaseInfo>
  </soap12:Body>
</soap12:Envelope>"""

        return self.call_custom_soap_service("PurchaseInfo", soap_envelope)
