"""Stream type classes for tap-sherpa."""
from __future__ import annotations

import typing as t
from importlib import resources
from typing import Dict, Any, Optional, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream
from singer_sdk.helpers._state import increment_state

from tap_serpaan.client import SherpaClient

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


class SherpaStream(Stream):
    """Base stream class for Sherpa streams."""
    def __init__(self, *args, **kwargs):
        """Initialize the stream."""
        super().__init__(*args, **kwargs)
        self.client = SherpaClient(
            wsdl_url=self.config["wsdl_url"],
            tap=self._tap,
        )

    def get_records(
        self,
        context: t.Optional[dict] = None,
    ) -> t.Iterable[dict]:
        # This method should be implemented by specific stream classes
        raise NotImplementedError("Stream classes must implement get_records")


# Import PaginatedStream after SherpaStream is defined to avoid circular imports
from tap_serpaan.pagination import PaginatedStream, PaginationMode


class ChangedItemsInformationStream(PaginatedStream):
    # Get changed items information
    name = "changed_items_information"
    primary_keys = ["ItemCode"]
    replication_key = "Token"
    schema = th.PropertiesList(
        th.Property("ItemCode", th.StringType),
        th.Property("Token", th.IntegerType),
        th.Property("ItemName", th.StringType),
        th.Property("ItemDescription", th.StringType),
        th.Property("ItemStatus", th.StringType),
        th.Property("EanCode", th.StringType),
        th.Property("CustomFields", th.ObjectType()),
        th.Property("Warehouses", th.ArrayType(th.ObjectType())),
        th.Property("ItemSuppliers", th.ArrayType(th.ObjectType())),
        th.Property("ItemAssemblies", th.ArrayType(th.ObjectType())),
        th.Property("ItemPurchases", th.ArrayType(th.ObjectType())),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    response_path = "ItemInformation"

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        yield from self.get_records_with_custom_client_method(
            "get_changed_items_information"
        )


class ChangedStockStream(PaginatedStream):
    # Get changed stocks
    name = "changed_stock"
    primary_keys = ["ItemCode", "WarehouseCode"]
    replication_key = "Token"
    schema = th.PropertiesList(
        th.Property("ItemCode", th.StringType),
        th.Property("Available", th.IntegerType),
        th.Property("Stock", th.IntegerType),
        th.Property("Reserved", th.IntegerType),
        th.Property("ItemStatus", th.StringType),
        th.Property("Token", th.IntegerType),
        th.Property("ExpectedDate", th.DateTimeType),
        th.Property("QtyWaitingToReceive", th.IntegerType),
        th.Property("FirstExpectedDate", th.DateTimeType),
        th.Property("FirstExpectedQtyWaitingToReceive", th.IntegerType),
        th.Property("LastModified", th.DateTimeType),
        th.Property("AvgPurchasePrice", th.NumberType),
        th.Property("WarehouseCode", th.StringType),
        th.Property("CostPrice", th.NumberType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    response_path = "ItemStockToken"


class ChangedSuppliersStream(PaginatedStream):
    # Get changed suppliers and then get supplier info for each supplier
    name = "changed_suppliers"
    primary_keys = ["ClientCode"]
    replication_key = "Token"
    schema = th.PropertiesList(
        th.Property("ClientCode", th.StringType),
        th.Property("Token", th.IntegerType),
        th.Property("Active", th.StringType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    response_path = "ClientCodeToken"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "client_code": record["ClientCode"],
        }


class SupplierInfoStream(SherpaStream):
    # Get supplier info for each supplier
    name = "supplier_info"
    parent_stream_type = ChangedSuppliersStream
    primary_keys = ["ClientCode"]
    schema = th.PropertiesList(
        th.Property("ClientCode", th.StringType),
        th.Property("ClientName", th.StringType),
        th.Property("ClientDescription", th.StringType),
        th.Property("ClientStatus", th.StringType),
        th.Property("ClientType", th.StringType),
        th.Property("ClientAddress", th.ObjectType()),
        th.Property("ClientContact", th.ObjectType()),
        th.Property("ClientBankAccount", th.ObjectType()),
        th.Property("ClientTaxInfo", th.ObjectType()),
        th.Property("ClientCustomFields", th.ObjectType()),
        th.Property("response_time", th.IntegerType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        """Get supplier info using the client_code from parent context."""
        client_code = context["client_code"]
        # Get detailed supplier info for this client code
        supplier_info = self.client.get_supplier_info(client_code)
        # Return the response directly - let the schema handle the structure
        yield supplier_info


class ChangedItemSuppliersWithDefaultsStream(PaginatedStream):
    # Get changed item supplierProducts with defaults
    name = "changed_item_suppliers_with_defaults"
    primary_keys = ["ItemCode", "ClientCode"]
    replication_key = "Token"
    schema = th.PropertiesList(
        th.Property("ItemCode", th.StringType),
        th.Property("ClientCode", th.StringType),
        th.Property("Token", th.IntegerType),
        th.Property("DefaultSupplier", th.StringType),
        th.Property("PurchasePrice", th.NumberType),
        th.Property("Currency", th.StringType),
        th.Property("LeadTime", th.IntegerType),
        th.Property("MinOrderQty", th.IntegerType),
        th.Property("MaxOrderQty", th.IntegerType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    response_path = "ItemSupplierToken"


class ChangedOrdersInformationStream(PaginatedStream):
    # Get changed orders information
    name = "changed_orders_information"
    primary_keys = ["OrderCode"]
    replication_key = "Token"
    schema = th.PropertiesList(
        th.Property("OrderCode", th.StringType),
        th.Property("Token", th.IntegerType),
        th.Property("OrderNumber", th.StringType),
        th.Property("OrderDate", th.DateTimeType),
        th.Property("OrderStatus", th.StringType),
        th.Property("CustomerCode", th.StringType),
        th.Property("CustomerName", th.StringType),
        th.Property("OrderLines", th.ArrayType(th.ObjectType())),
        th.Property("TotalAmount", th.NumberType),
        th.Property("Currency", th.StringType),
        th.Property("WarehouseCode", th.StringType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    response_path = "OrderInformation"

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        yield from self.get_records_with_custom_client_method(
            "get_changed_orders_information"
        )


class ChangedPurchasesStream(PaginatedStream):
    # Get changed purchases
    name = "changed_purchases"
    primary_keys = ["PurchaseCode"]
    replication_key = "Token"
    schema = th.PropertiesList(
        th.Property("PurchaseCode", th.StringType),
        th.Property("OrderNumber", th.StringType),
        th.Property("Token", th.IntegerType),
        th.Property("PurchaseStatus", th.StringType),
        th.Property("WarehouseCode", th.StringType),
        th.Property("response_time", th.IntegerType),
    ).to_dict()
    response_path = "PurchaseCodeToken"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "purchase_number": record["OrderNumber"],
        }


class PurchaseInfoStream(SherpaStream):
    # Get purchase info for each purchase
    name = "purchase_info"
    parent_stream_type = ChangedPurchasesStream
    primary_keys = ["PurchaseNumber"]
    schema = th.PropertiesList(
        th.Property("PurchaseNumber", th.StringType),
        th.Property("PurchaseCode", th.StringType),
        th.Property("OrderNumber", th.StringType),
        th.Property("PurchaseDate", th.DateTimeType),
        th.Property("PurchaseStatus", th.StringType),
        th.Property("SupplierCode", th.StringType),
        th.Property("SupplierName", th.StringType),
        th.Property("WarehouseCode", th.StringType),
        th.Property("TotalAmount", th.NumberType),
        th.Property("Currency", th.StringType),
        th.Property("PurchaseLines", th.ArrayType(th.ObjectType())),
        th.Property("response_time", th.IntegerType),
    ).to_dict()

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        """Get purchase info using the purchase_number from parent context."""
        purchase_number = context["purchase_number"]
        # Get detailed purchase info for this purchase number
        purchase_info = self.client.get_purchase_info(purchase_number)
        # Return the response directly - let the schema handle the structure
        yield purchase_info


