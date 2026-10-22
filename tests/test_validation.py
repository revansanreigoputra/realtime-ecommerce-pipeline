from pipeline.validation import validate_event


def valid_event():
    """Membuat contoh event yang memenuhi aturan validasi."""
    return {
        "event_id": 1,
        "event_type": "retail_transaction",
        "source": "online_retail_ii",
        "data": {
            "Invoice": "489434",
            "StockCode": "85048",
            "Quantity": 12,
            "InvoiceDate": "2009-12-01 07:45:00",
            "Price": 6.95,
            "Country": "United Kingdom",
        },
    }


def test_valid_event_is_accepted():
    event = valid_event()

    valid, reason = validate_event(event)

    assert valid is True
    assert reason is None


def test_non_dictionary_event_is_rejected():
    valid, reason = validate_event(["bukan", "object"])

    assert valid is False
    assert reason == "Event harus berupa JSON object"


def test_missing_required_event_fields_is_rejected():
    event = valid_event()
    del event["event_type"]

    valid, reason = validate_event(event)

    assert valid is False
    assert "event_type" in reason


def test_unknown_event_type_is_rejected():
    event = valid_event()
    event["event_type"] = "unknown_event"

    valid, reason = validate_event(event)

    assert valid is False
    assert reason == "event_type tidak dikenal"


def test_non_dictionary_data_is_rejected():
    event = valid_event()
    event["data"] = "bukan object"

    valid, reason = validate_event(event)

    assert valid is False
    assert reason == "Field data harus berupa object"


def test_missing_transaction_fields_is_rejected():
    event = valid_event()
    del event["data"]["Price"]

    valid, reason = validate_event(event)

    assert valid is False
    assert "Price" in reason
