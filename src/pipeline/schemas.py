from typing import Any, TypedDict


class RetailTransactionData(TypedDict):
    """Field transaksi yang digunakan pipeline Online Retail II."""

    Invoice: Any
    StockCode: Any
    Quantity: Any
    InvoiceDate: Any
    Price: Any
    Country: Any


class RetailTransactionEvent(TypedDict):
    """Struktur event yang dikirim producer ke Kafka."""

    event_id: int
    event_type: str
    source: str
    data: dict[str, Any]
