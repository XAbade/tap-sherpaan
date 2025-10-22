"""Stream type classes for tap-sherpa."""
from __future__ import annotations

import typing as t
from importlib import resources
from typing import Dict, Any, Optional, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream
from singer_sdk.helpers._state import increment_state

from tap_sherpaan.client import SherpaClient

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class SherpaStream(Stream):
    """Base stream class for Sherpa streams."""
    def __init__(self, *args, **kwargs):
        """Initialize the stream."""
        super().__init__(*args, **kwargs)
        self.client = SherpaClient(
            shop_id=self.config["shop_id"],
            tap=self._tap,
        )

    def get_records(
        self,
        context: t.Optional[dict] = None,
    ) -> t.Iterable[dict]:
        # This method should be implemented by specific stream classes
        raise NotImplementedError("Stream classes must implement get_records")


# Import PaginatedStream after SherpaStream is defined to avoid circular imports
from tap_sherpaan.pagination import PaginatedStream, PaginationMode


class ChangedItemsInformationStream(PaginatedStream):
    # Get changed items information
    name = "changed_items_information"
    primary_keys = ["ItemCode"]
    replication_key = "Token"
    response_path = "ItemCodeTokenItemInformation"
    schema = th.PropertiesList(
        # Basic item information
        th.Property("ItemCode", th.StringType),
        th.Property("ItemStatus", th.StringType),
        th.Property("Token", th.StringType),
        # General item details
        th.Property("ItemType", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("Brand", th.StringType),
        th.Property("AutoStockLevel", th.BooleanType),
        th.Property("Dropship", th.BooleanType),
        th.Property("HideOnPicklist", th.BooleanType),
        th.Property("HideOnInvoice", th.BooleanType),
        th.Property("HideOnReturnDocument", th.BooleanType),
        th.Property("PrintLabelsReceivedPurchaseItems", th.BooleanType),
        th.Property("CostPrice", th.StringType),
        th.Property("Price", th.StringType),
        th.Property("VatCode", th.StringType),
        th.Property("StockPeriod", th.StringType),
        th.Property("OrderVolume", th.StringType),
        th.Property("OrderVolumeCeilFrom", th.StringType),
        th.Property("PriceIncl", th.StringType),
        th.Property("Weight", th.StringType),
        th.Property("Length", th.StringType),
        th.Property("Width", th.StringType),
        th.Property("Height", th.StringType),
        th.Property("DateAdded", th.DateTimeType),
        th.Property("AvgPurchasePrice", th.StringType),
        th.Property("StockInAllWarehouses", th.StringType),
        th.Property("ReservedInAllWarehouses", th.StringType),
        th.Property("AvailableStockInAllWarehouses", th.StringType),
        # EAN codes (stored as JSON string)
        th.Property("EanCode", th.StringType),
        # Custom fields (stored as JSON string)
        th.Property("CustomFields", th.StringType),
        # Warehouse information (stored as JSON string)
        th.Property("Warehouses", th.StringType),
        # Supplier information (stored as JSON string)
        th.Property("ItemSuppliers", th.StringType),
        # Assembly information (stored as JSON string)
        th.Property("ItemAssemblies", th.StringType),
        # Purchase information (stored as JSON string)
        th.Property("ItemPurchases", th.StringType)
    ).to_dict()

    def get_changed_items_information(self, token: int, count: int = 200) -> str:
        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:ChangedItemsInformation xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.config["security_code"]}</tns:securityCode>
      <tns:token>{token}</tns:token>
      <tns:count>{count}</tns:count>
      <tns:itemInformationTypes>
        <tns:ItemInformationType>General</tns:ItemInformationType>
        <tns:ItemInformationType>EanCode</tns:ItemInformationType>
        <tns:ItemInformationType>CustomFields</tns:ItemInformationType>
        <tns:ItemInformationType>Warehouses</tns:ItemInformationType>
        <tns:ItemInformationType>ItemSuppliers</tns:ItemInformationType>
        <tns:ItemInformationType>ItemAssemblies</tns:ItemInformationType>
        <tns:ItemInformationType>ItemPurchases</tns:ItemInformationType>
      </tns:itemInformationTypes>
    </tns:ChangedItemsInformation>
  </soap12:Body>
</soap12:Envelope>"""

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        yield from self.get_records_with_custom_client_method(
            "get_changed_items_information"
        )


class ChangedStockStream(PaginatedStream):
    # Get changed stocks
    name = "changed_stock"
    primary_keys = ["ItemCode", "WarehouseCode"]
    replication_key = "Token"
    response_path = "ItemStockToken"
    schema = th.PropertiesList(
        th.Property("ItemCode", th.StringType),
        th.Property("Available", th.StringType),
        th.Property("Stock", th.StringType),
        th.Property("Reserved", th.StringType),
        th.Property("ItemStatus", th.StringType),
        th.Property("ExpectedDate", th.DateTimeType),
        th.Property("QtyWaitingToReceive", th.StringType),
        th.Property("FirstExpectedDate", th.DateTimeType),
        th.Property("FirstExpectedQtyWaitingToReceive", th.StringType),
        th.Property("LastModified", th.DateTimeType),
        th.Property("AvgPurchasePrice", th.StringType),
        th.Property("WarehouseCode", th.StringType),
        th.Property("CostPrice", th.StringType),
        th.Property("Token", th.StringType)
    ).to_dict()

    def get_changed_stock(self, token: int, maxResult: int = 200) -> str:
        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:ChangedStock xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.config["security_code"]}</tns:securityCode>
      <tns:token>{token}</tns:token>
      <tns:maxResult>{maxResult}</tns:maxResult>
    </tns:ChangedStock>
  </soap12:Body>
</soap12:Envelope>"""

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        yield from self.get_records_with_custom_client_method(
            "get_changed_stock"
        )


class ChangedSuppliersStream(PaginatedStream):
    # Get changed suppliers and then get supplier info for each supplier
    name = "changed_suppliers"
    primary_keys = ["ClientCode"]
    replication_key = "Token"
    response_path = "ClientCodeToken"
    schema = th.PropertiesList(
        th.Property("ClientCode", th.StringType),
        th.Property("Active", th.StringType),
        th.Property("Token", th.StringType)
    ).to_dict()

    def get_changed_suppliers(self, token: int, count: int = 200) -> str:
        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:ChangedSuppliers xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.config["security_code"]}</tns:securityCode>
      <tns:token>{token}</tns:token>
      <tns:count>{count}</tns:count>
    </tns:ChangedSuppliers>
  </soap12:Body>
</soap12:Envelope>"""

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        yield from self.get_records_with_custom_client_method(
            "get_changed_suppliers"
        )

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "client_code": record["ClientCode"],
        }

class SupplierInfoStream(PaginatedStream):
    # Get supplier info for each supplier
    name = "supplier_info"
    parent_stream_type = ChangedSuppliersStream
    primary_keys = ["SupplierCode"]
    response_path = "ResponseValue"
    paginate = False
    schema = th.PropertiesList(
        th.Property("SupplierCode", th.StringType),
        th.Property("Token", th.StringType),
        th.Property("Remarks", th.StringType),
        th.Property("CustomFields", th.StringType),
        # Address fields (flattened from BillingAddress/ShipmentAddess)
        th.Property("AddressType", th.StringType),
        th.Property("Gender", th.StringType),
        th.Property("Name", th.StringType),
        th.Property("Company", th.StringType),
        th.Property("Street", th.StringType),
        th.Property("HouseNumber", th.StringType),
        th.Property("PostalCode", th.StringType),
        th.Property("City", th.StringType),
        th.Property("CountryCode", th.StringType),
        th.Property("CountryName", th.StringType),
        th.Property("AddressLine1", th.StringType),
        th.Property("AddressLine2", th.StringType),
        th.Property("AddressLine3", th.StringType),
        th.Property("EmailAddressIsInvalid", th.StringType),
        th.Property("AllowMailing", th.StringType),
        # Supplier settings fields (flattened from SupplierSettings)
        th.Property("OrderPeriod", th.StringType),
        th.Property("DeliveryPeriod", th.StringType),
        th.Property("AutoPreferredItemSupplier", th.StringType)
    ).to_dict()

    def get_supplier_info(self, token: int = 0, **kwargs) -> str:
        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:SupplierInfo xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.config["security_code"]}</tns:securityCode>
      <tns:supplierCode>{self._current_client_code}</tns:supplierCode>
    </tns:SupplierInfo>
  </soap12:Body>
</soap12:Envelope>"""

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        """Get supplier info using the client_code from parent context."""
        # Store client_code in instance variable so get_supplier_info can access it
        self._current_client_code = context["client_code"]
        # Use the same approach as other streams - this will automatically process the response
        yield from self.get_records_with_custom_client_method("get_supplier_info")


class ChangedItemSuppliersWithDefaultsStream(PaginatedStream):
    # Get changed item supplierProducts with defaults
    name = "changed_item_suppliers_with_defaults"
    primary_keys = ["ItemCode", "ClientCode"]
    replication_key = "Token"
    response_path = "SupplierItemCodeToken"
    schema = th.PropertiesList(
        th.Property("SupplierCode", th.StringType),
        th.Property("SupplierItemCode", th.StringType),
        th.Property("ItemCode", th.StringType),
        th.Property("SupplierDescription", th.StringType),
        th.Property("SupplierStock", th.StringType),
        th.Property("SupplierPrice", th.StringType),
        th.Property("OrderPeriod", th.StringType),
        th.Property("DeliveryPeriod", th.StringType),
        th.Property("Preferred", th.StringType),
        th.Property("Token", th.StringType),
        th.Property("AvailableFrom", th.StringType),
        th.Property("SupplierItemStatus", th.StringType),
        th.Property("VatCode", th.StringType),
        th.Property("LastModified", th.StringType),
        th.Property("MinPurchaseQty", th.StringType),
        th.Property("SupplierPurchaseQty", th.StringType),
        th.Property("SupplierPurchaseQtyMultiplier", th.StringType)
    ).to_dict()

    def get_changed_item_suppliers_with_defaults(self, token: int, count: int = 200) -> str:
        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:ChangedItemSuppliersWithDefaults xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.config["security_code"]}</tns:securityCode>
      <tns:token>{token}</tns:token>
      <tns:count>{count}</tns:count>
    </tns:ChangedItemSuppliersWithDefaults>
  </soap12:Body>
</soap12:Envelope>"""

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        yield from self.get_records_with_custom_client_method(
            "get_changed_item_suppliers_with_defaults"
        )

class ChangedOrdersInformationStream(PaginatedStream):
    # Get changed orders information
    name = "changed_orders_information"
    primary_keys = ["OrderCode"]
    replication_key = "Token"
    response_path = "OrderNumberTokenOrderInformation"
    schema = th.PropertiesList(
        # Top-level fields
        th.Property("OrderNumber", th.StringType),
        th.Property("Token", th.StringType),
        th.Property("OrderStatus", th.StringType),
        # General section fields
        th.Property("OrderDate", th.DateTimeType),
        th.Property("InvoiceDate", th.DateTimeType),
        th.Property("SendInvoiceByEmail", th.BooleanType),
        th.Property("NumberOfColli", th.StringType),
        th.Property("Priority", th.BooleanType),
        th.Property("ShippingDate", th.DateTimeType),
        th.Property("PricesIncl", th.BooleanType),
        th.Property("OrderAmountInclVAT", th.StringType),
        th.Property("OrderAmountInclVATInclBackOrderItems", th.StringType),
        th.Property("Paid", th.StringType),
        th.Property("ElectronicPaid", th.StringType),
        th.Property("AmountDue", th.StringType),
        th.Property("Margin", th.StringType),
        th.Property("WarehouseCode", th.StringType),
        th.Property("OrderWarning", th.StringType),
        th.Property("PaymentMethodCode", th.StringType),
        th.Property("ParcelServiceCode", th.StringType),
        th.Property("ParcelTypeCode", th.StringType),
        # Complex nested objects (stored as JSON strings)
        th.Property("OrderLines", th.StringType)
    ).to_dict()

    def get_changed_orders_information(self, token: int, count: int = 200) -> str:
        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:ChangedOrdersInformation xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.config["security_code"]}</tns:securityCode>
      <tns:token>{token}</tns:token>
      <tns:count>{count}</tns:count>
      <tns:orderInformationTypes>
        <tns:OrderInformationType>General</tns:OrderInformationType>
        <tns:OrderInformationType>OrderLines</tns:OrderInformationType>
      </tns:orderInformationTypes>
    </tns:ChangedOrdersInformation>
  </soap12:Body>
</soap12:Envelope>"""

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        yield from self.get_records_with_custom_client_method(
            "get_changed_orders_information"
        )


class ChangedPurchasesStream(PaginatedStream):
    # Get changed purchases
    name = "changed_purchases"
    primary_keys = ["PurchaseCode"]
    replication_key = "Token"
    response_path = "PurchaseCodeToken"
    # Class variable to store unique group IDs
    _unique_order_numbers = set()
    schema = th.PropertiesList(
        th.Property("PurchaseCode", th.StringType),
        th.Property("OrderNumber", th.StringType),
        th.Property("Token", th.StringType),
        th.Property("PurchaseStatus", th.StringType),
        th.Property("WarehouseCode", th.StringType)
    ).to_dict()

    def get_changed_purchases(self, token: int, count: int = 200) -> str:
        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:ChangedPurchases xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.config["security_code"]}</tns:securityCode>
      <tns:token>{token}</tns:token>
      <tns:count>{count}</tns:count>
    </tns:ChangedPurchases>
  </soap12:Body>
</soap12:Envelope>"""

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        for record in self.get_records_with_custom_client_method("get_changed_purchases"):
            # Only yield records that have OrderNumber (discard all others as useless)
            if record.get("OrderNumber"):
                yield record

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return context for child streams."""
        purchase_number = record.get("OrderNumber")
        if not purchase_number:
            return None
        
        # Only return context for unique order numbers to avoid duplicate child stream calls
        if purchase_number in self._unique_order_numbers:
            return None
        
        self._unique_order_numbers.add(purchase_number)
        return {"purchase_number": purchase_number}

    def _sync_children(self, child_context: dict) -> None:
        if child_context is not None:
            super()._sync_children(child_context)



class PurchaseInfoStream(PaginatedStream):
    # Get purchase info for each purchase
    name = "purchase_info"
    parent_stream_type = ChangedPurchasesStream
    primary_keys = ["PurchaseOrderNumber"]
    response_path = "ResponseValue"
    paginate = False
    schema = th.PropertiesList(
        th.Property("SupplierCode", th.StringType),
        th.Property("PurchaseOrderNumber", th.StringType),
        th.Property("PurchaseDate", th.StringType),
        th.Property("PurchaseStatus", th.DateTimeType),
        th.Property("Reference", th.StringType),
        th.Property("WarehouseCode", th.StringType),
        th.Property("PurchaseLine", th.StringType)
    ).to_dict()

    def get_purchase_info(self, token: int = 0, **kwargs) -> str:
        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:PurchaseInfo xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.config["security_code"]}</tns:securityCode>
      <tns:purchaseNumber>{self._current_purchase_number}</tns:purchaseNumber>
    </tns:PurchaseInfo>
  </soap12:Body>
</soap12:Envelope>"""

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        """Get purchase info using the purchase_number from parent context."""
        # Store purchase_number in instance variable so get_purchase_info can access it
        self._current_purchase_number = context["purchase_number"]
        # Use the same approach as other streams - this will automatically process the response
        yield from self.get_records_with_custom_client_method("get_purchase_info")
