CREATE OR REPLACE VIEW v_retail_sales_summary AS
SELECT
    COUNT(*) AS total_events,

    SUM(
        (payload->'data'->>'Quantity')::NUMERIC
    ) AS total_quantity,

    ROUND(
        SUM(
            (payload->'data'->>'Quantity')::NUMERIC
            *
            (payload->'data'->>'Price')::NUMERIC
        ),
        2
    ) AS estimated_gross_value,

    COUNT(
        DISTINCT payload->'data'->>'Country'
    ) AS total_countries,

    MIN(
        (payload->'data'->>'InvoiceDate')::TIMESTAMP
    ) AS first_invoice_date,

    MAX(
        (payload->'data'->>'InvoiceDate')::TIMESTAMP
    ) AS last_invoice_date

FROM raw_events
WHERE
    payload->'data'->>'Quantity' IS NOT NULL
    AND payload->'data'->>'Price' IS NOT NULL
    AND payload->'data'->>'InvoiceDate' IS NOT NULL;
