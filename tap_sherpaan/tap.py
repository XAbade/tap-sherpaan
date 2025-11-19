"""Sherpa tap class."""

from __future__ import annotations

from typing import List
import click
from singer_sdk import Tap
from singer_sdk import typing as th

from tap_sherpaan import streams


class TapSherpaan(Tap):
    """Sherpa tap class."""
    
    name = "tap-sherpaan"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "base_url",
            th.StringType,
            required=False,
            default="https://sherpaservices-prd.sherpacloud.eu",
            description="Base URL for the Sherpa SOAP service (override for non-production environments)",
        ),
        th.Property(
            "shop_id",
            th.StringType,
            required=True,
            description="The shop ID for the Sherpa SOAP service",
        ),
        th.Property(
            "security_code",
            th.StringType,
            required=True,
            secret=True,
            description="Security code for authentication",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "chunk_size",
            th.IntegerType,
            description="Number of records to process in each chunk",
            default=200,
        ),
        th.Property(
            "max_retries",
            th.IntegerType,
            description="Maximum number of retry attempts for failed requests",
            default=3,
        ),
        th.Property(
            "retry_wait_min",
            th.IntegerType,
            description="Minimum wait time between retries in seconds",
            default=4,
        ),
        th.Property(
            "retry_wait_max",
            th.IntegerType,
            description="Maximum wait time between retries in seconds",
            default=10,
        ),
    ).to_dict()

    @classmethod
    def cli(cls, *args, **kwargs):
        """Handle command line execution."""
        @click.option("--properties", help="DEPRECATED. Path to a properties JSON file.")
        def cli_func(*args, **kwargs):
            kwargs.pop("properties", None)
            super(TapSherpaan, cls).cli(*args, **kwargs)

        cli_func(*args, **kwargs)

    def discover_streams(self) -> List[streams.SherpaStream]:
        """Return a list of discovered streams."""
        return [
            streams.ChangedItemsInformationStream(self),
            streams.ChangedStockStream(self),
            streams.ChangedSuppliersStream(self),
            streams.SupplierInfoStream(self),
            streams.ChangedItemSuppliersWithDefaultsStream(self),
            streams.ChangedOrdersInformationStream(self),
            streams.ChangedPurchasesStream(self),
            streams.PurchaseInfoStream(self),
        ]


if __name__ == "__main__":
    TapSherpaan.cli()
