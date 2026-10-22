def validate_event(event):
    """Mengembalikan (valid, alasan_error)."""

    if not isinstance(event, dict):
        return False, "Event harus berupa JSON object"

    required_fields = {
        "event_id",
        "event_type",
        "source",
        "data",
    }

    missing = required_fields - event.keys()

    if missing:
        return False, f"Field event hilang: {sorted(missing)}"

    if event["event_type"] != "retail_transaction":
        return False, "event_type tidak dikenal"

    if not isinstance(event["data"], dict):
        return False, "Field data harus berupa object"

    required_data = {
        "Invoice",
        "StockCode",
        "Quantity",
        "InvoiceDate",
        "Price",
        "Country",
    }

    missing_data = required_data - event["data"].keys()

    if missing_data:
        return False, f"Field transaksi hilang: {sorted(missing_data)}"

    return True, None
