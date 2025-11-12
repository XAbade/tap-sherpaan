"""Stream type classes for tap-sherpaan."""

from __future__ import annotations
import html
from typing import Dict, Any, Iterable, Optional
from singer_sdk import typing as th
from tap_sherpaan.client import SherpaStream


class ChangedItemsInformationStream(SherpaStream):
    """Stream for changed items information."""
    
    name = "changed_items_information"
    primary_keys = ["ItemCode"]
    replication_key = "Token"
    schema = th.PropertiesList(
        th.Property("ItemCode", th.StringType),
        th.Property("ItemStatus", th.StringType),
        th.Property("Token", th.StringType),
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
        th.Property("EanCode", th.StringType),
        th.Property("CustomFields", th.StringType),
        th.Property("Warehouses", th.StringType),
        th.Property("ItemSuppliers", th.StringType),
        th.Property("ItemAssemblies", th.StringType),
        th.Property("ItemPurchases", th.StringType),
    ).to_dict()

    def _get_soap_envelope(self, token: int, count: int = 200) -> str:
        """Generate SOAP envelope for ChangedItemsInformation."""
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
        """Get records using token-based pagination."""
        page_size = self.config.get("chunk_size", 200)
        yield from self.get_records_with_token_pagination(
            get_soap_envelope=self._get_soap_envelope,
            service_name="ChangedItemsInformation",
            items_key="ItemCodeTokenItemInformation",
            context=context,
            page_size=page_size,
        )


class ChangedStockStream(SherpaStream):
    """Stream for changed stock."""
    
    name = "changed_stock"
    primary_keys = ["ItemCode", "WarehouseCode"]
    replication_key = "Token"
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

    def _get_soap_envelope(self, token: int, count: int = 200) -> str:
        """Generate SOAP envelope for ChangedStock."""
        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:ChangedStock xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.config["security_code"]}</tns:securityCode>
      <tns:token>{token}</tns:token>
      <tns:maxResult>{count}</tns:maxResult>
    </tns:ChangedStock>
  </soap12:Body>
</soap12:Envelope>"""

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        """Get records using token-based pagination."""
        page_size = self.config.get("chunk_size", 200)
        yield from self.get_records_with_token_pagination(
            get_soap_envelope=self._get_soap_envelope,
            service_name="ChangedStock",
            items_key="ItemStockToken",
            context=context,
            page_size=page_size,
        )


class ChangedSuppliersStream(SherpaStream):
    """Stream for changed suppliers."""
    
    name = "changed_suppliers"
    primary_keys = ["ClientCode"]
    replication_key = "Token"
    schema = th.PropertiesList(
        th.Property("ClientCode", th.StringType),
        th.Property("Active", th.StringType),
        th.Property("Token", th.StringType)
    ).to_dict()

    def _get_soap_envelope(self, token: int, count: int = 200) -> str:
        """Generate SOAP envelope for ChangedSuppliers."""
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
        """Get records using token-based pagination."""
        page_size = self.config.get("chunk_size", 200)
        yield from self.get_records_with_token_pagination(
            get_soap_envelope=self._get_soap_envelope,
            service_name="ChangedSuppliers",
            items_key="ClientCodeToken",
            context=context,
            page_size=page_size,
        )

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "client_code": record["ClientCode"],
        }


class SupplierInfoStream(SherpaStream):
    """Stream for supplier info."""
    
    name = "supplier_info"
    parent_stream_type = ChangedSuppliersStream
    primary_keys = ["ClientCode"]
    paginate = False
    schema = th.PropertiesList(
        th.Property("SupplierCode", th.StringType),
        th.Property("Token", th.StringType),
        th.Property("Remarks", th.StringType),
        th.Property("CustomFields", th.StringType),
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
        th.Property("OrderPeriod", th.StringType),
        th.Property("DeliveryPeriod", th.StringType),
        th.Property("AutoPreferredItemSupplier", th.StringType)
    ).to_dict()

    def _get_soap_envelope(self, token: int = 0, count: int = 200, **kwargs) -> str:
        """Generate SOAP envelope for SupplierInfo."""
        encoded_supplier_code = html.escape(self._current_client_code)
        return f"""<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
  <soap12:Body>
    <tns:SupplierInfo xmlns:tns="http://sherpa.sherpaan.nl/">
      <tns:securityCode>{self.config["security_code"]}</tns:securityCode>
      <tns:supplierCode>{encoded_supplier_code}</tns:supplierCode>
    </tns:SupplierInfo>
  </soap12:Body>
</soap12:Envelope>"""

    def get_records(self, context: Optional[dict] = None) -> Iterable[dict]:
        """Get supplier info using the client_code from parent context."""
        self._current_client_code = context["client_code"]
        page_size = self.config.get("chunk_size", 200)
        yield from self.get_records_with_token_pagination(
            get_soap_envelope=self._get_soap_envelope,
            service_name="SupplierInfo",
            items_key="ResponseValue",
            context=context,
            page_size=page_size,
        )


class ChangedItemSuppliersWithDefaultsStream(SherpaStream):
    """Stream for changed item suppliers with defaults."""
    
    name = "changed_item_suppliers_with_defaults"
    primary_keys = ["ItemCode", "ClientCode"]
    replication_key = "Token"
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

    def _get_soap_envelope(self, token: int, count: int = 200) -> str:
        """Generate SOAP envelope for ChangedItemSuppliersWithDefaults."""
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
        """Get records using token-based pagination."""
        page_size = self.config.get("chunk_size", 200)
        yield from self.get_records_with_token_pagination(
            get_soap_envelope=self._get_soap_envelope,
            service_name="ChangedItemSuppliersWithDefaults",
            items_key="SupplierItemCodeToken",
            context=context,
            page_size=page_size,
        )


class ChangedOrdersInformationStream(SherpaStream):
    """Stream for changed orders information."""
    
    name = "changed_orders_information"
    primary_keys = ["OrderCode"]
    replication_key = "Token"
    schema = th.PropertiesList(
        th.Property("OrderNumber", th.StringType),
        th.Property("Token", th.StringType),
        th.Property("OrderStatus", th.StringType),
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
        th.Property("OrderLines", th.StringType)
    ).to_dict()

    def _get_soap_envelope(self, token: int, count: int = 200) -> str:
        """Generate SOAP envelope for ChangedOrdersInformation."""
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
        """Get records using token-based pagination."""
        page_size = self.config.get("chunk_size", 200)
        yield from self.get_records_with_token_pagination(
            get_soap_envelope=self._get_soap_envelope,
            service_name="ChangedOrdersInformation",
            items_key="OrderNumberTokenOrderInformation",
            context=context,
            page_size=page_size,
        )


class ChangedPurchasesStream(SherpaStream):
    """Stream for changed purchases."""
    
    name = "changed_purchases"
    primary_keys = ["PurchaseCode"]
    replication_key = "Token"
    _unique_order_numbers = set()
    schema = th.PropertiesList(
        th.Property("PurchaseCode", th.StringType),
        th.Property("OrderNumber", th.StringType),
        th.Property("Token", th.StringType),
        th.Property("PurchaseStatus", th.StringType),
        th.Property("WarehouseCode", th.StringType)
    ).to_dict()

    def _get_soap_envelope(self, token: int, count: int = 200) -> str:
        """Generate SOAP envelope for ChangedPurchases."""
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
        """Get records using token-based pagination."""
        page_size = self.config.get("chunk_size", 200)
        for record in self.get_records_with_token_pagination(
            get_soap_envelope=self._get_soap_envelope,
            service_name="ChangedPurchases",
            items_key="PurchaseCodeToken",
            context=context,
            page_size=page_size,
        ):
            # Only yield records that have OrderNumber
            if record.get("OrderNumber"):
                yield record

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return context for child streams."""
        purchase_number = record.get("OrderNumber")
        if not purchase_number:
            return None
        
        # Only return context for unique order numbers
        if purchase_number in self._unique_order_numbers:
            return None
        
        self._unique_order_numbers.add(purchase_number)
        return {"purchase_number": purchase_number}

    def _sync_children(self, child_context: dict) -> None:
        """Sync child streams only if context is not None."""
        if child_context is not None:
            super()._sync_children(child_context)


class PurchaseInfoStream(SherpaStream):
    """Stream for purchase info."""
    
    name = "purchase_info"
    parent_stream_type = ChangedPurchasesStream
    primary_keys = ["PurchaseOrderNumber"]
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

    def _get_soap_envelope(self, token: int = 0, count: int = 200, **kwargs) -> str:
        """Generate SOAP envelope for PurchaseInfo."""
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
        self._current_purchase_number = context["purchase_number"]
        page_size = self.config.get("chunk_size", 200)
        yield from self.get_records_with_token_pagination(
            get_soap_envelope=self._get_soap_envelope,
            service_name="PurchaseInfo",
            items_key="ResponseValue",
            context=context,
            page_size=page_size,
        )
