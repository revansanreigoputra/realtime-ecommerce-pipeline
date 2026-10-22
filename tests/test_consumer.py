from pipeline.consumer import fingerprint_event


def sample_event():
    """Membuat contoh event transaksi untuk pengujian."""
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


def test_same_event_produces_same_fingerprint():
    event = sample_event()

    fingerprint_1 = fingerprint_event(event)
    fingerprint_2 = fingerprint_event(event)

    assert fingerprint_1 == fingerprint_2


def test_different_event_produces_different_fingerprint():
    event_1 = sample_event()
    event_2 = sample_event()

    event_2["data"]["Quantity"] = 20

    fingerprint_1 = fingerprint_event(event_1)
    fingerprint_2 = fingerprint_event(event_2)

    assert fingerprint_1 != fingerprint_2


def test_fingerprint_is_sha256_hex():
    fingerprint = fingerprint_event(sample_event())

    assert len(fingerprint) == 64
    assert all(
        character in "0123456789abcdef"
        for character in fingerprint
    )
