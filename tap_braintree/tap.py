"""braintree tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_braintree.streams import (
    BraintreeStream,
    TransactionsStream,
    SubscriptionsStream,
    PlansStream,
)

STREAM_TYPES = [
    TransactionsStream,
    SubscriptionsStream,
    PlansStream,
]


class TapBraintree(Tap):
    """braintree tap class."""
    name = "tap-braintree"

    config_jsonschema = th.PropertiesList(
        th.Property("merchant_id", th.StringType, required=True),
        th.Property("public_key", th.StringType, required=True),
        th.Property("private_key", th.StringType, required=True),
        th.Property("start_date", th.DateTimeType, required=True),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
