import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# =========================================================
# Konfigurasi aplikasi
# =========================================================
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pipeline.db import get_connection


st.set_page_config(
    page_title="E-Commerce Stream Monitor",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)


# =========================================================
# Styling
# =========================================================
st.markdown(
    """
    <style>
    .block-container {
        max-width: 1550px;
        padding-top: 1.6rem;
        padding-bottom: 2.4rem;
    }

    [data-testid="stSidebar"] {
        border-right: 1px solid rgba(128, 128, 128, 0.18);
    }

    .brand {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 4px;
    }

    .brand-icon {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 46px;
        height: 46px;
        border-radius: 13px;
        background: linear-gradient(135deg, #2563eb, #0d9488);
        color: white;
        font-size: 24px;
    }

    .brand-title {
        font-size: 1.12rem;
        font-weight: 750;
        line-height: 1.25;
    }

    .brand-subtitle {
        color: #94a3b8;
        font-size: 0.83rem;
        margin-top: 3px;
    }

    .hero {
        padding: 1.65rem 1.8rem;
        border-radius: 18px;
        background: linear-gradient(115deg, #132238 0%, #173d50 58%, #126b67 100%);
        color: #ffffff;
        margin: 0.4rem 0 1.15rem 0;
        margin-top:1.5rem;
        border: 1px solid rgba(255,255,255,0.08);
    }

    .hero-eyebrow {
        color: #99f6e4;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        font-size: 0.72rem;
        font-weight: 750;
        margin-bottom: 0.6rem;
    }

    .hero h1 {
        color: #ffffff;
        font-size: clamp(1.65rem, 3vw, 2.25rem);
        font-weight: 800;
        margin: 0 0 0.55rem 0;
        line-height: 1.2;
    }

    .hero p {
        color: #dbeafe;
        margin: 0;
        font-size: 0.94rem;
        line-height: 1.55;
    }

    .section-subtitle {
        color: #94a3b8;
        font-size: 0.88rem;
        margin-top: -0.45rem;
        margin-bottom: 1rem;
    }

    div[data-testid="stMetric"] {
        padding: 1rem 1.05rem;
        border: 1px solid rgba(148, 163, 184, 0.22);
        border-radius: 15px;
        background: rgba(148, 163, 184, 0.045);
        min-height: 122px;
    }

    div[data-testid="stMetricLabel"] {
        font-size: 0.88rem;
    }

    div[data-testid="stMetricValue"] {
        font-size: clamp(1.35rem, 2vw, 1.85rem);
        font-weight: 750;
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid rgba(148, 163, 184, 0.2);
        border-radius: 12px;
        overflow: hidden;
    }

    div.stButton > button,
    div.stDownloadButton > button {
        border-radius: 10px;
        font-weight: 650;
        min-height: 2.65rem;
    }

    div[data-testid="stTabs"] button {
        font-weight: 650;
    }

    .footnote {
        color: #94a3b8;
        font-size: 0.78rem;
        text-align: center;
        padding-top: 0.6rem;
    }

    hr {
        margin: 1.25rem 0;
        border-color: rgba(148, 163, 184, 0.22);
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# =========================================================
# Fungsi bantu
# =========================================================
def read_query(query: str) -> pd.DataFrame:
    """Jalankan query baca-saja dan kembalikan hasil sebagai DataFrame."""
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)


def format_integer(value) -> str:
    """Format angka bulat dengan pemisah ribuan."""
    return f"{int(value):,}"


def format_money(value) -> str:
    """Format angka uang/estimasi dengan dua angka desimal."""
    return f"{float(value):,.2f}"


# =========================================================
# Sidebar dan navigasi
# =========================================================
with st.sidebar:
    st.markdown(
        """
        <div class="brand">
            <div class="brand-icon">📡</div>
            <div>
                <div class="brand-title">Stream Monitor</div>
                <div class="brand-subtitle">E-Commerce Data Platform</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    page = st.radio(
        "🧭 NAVIGASI",
        ["🏠 Overview", "🧾 Transaksi", "🚨 Dead Letter Queue"],
        index=0,
        label_visibility="visible",
    )

    st.divider()
    st.markdown("**🔌 Alur pipeline**")
    st.caption("🐍 Python  →  📨 Kafka  →  🐘 PostgreSQL")
    st.caption("🧪 Mode: simulasi streaming dari dataset historis")

    if st.button("🔄 Refresh data", use_container_width=True):
        st.rerun()

    st.divider()
    st.caption("E-Commerce Stream Monitor")
    st.caption("Data ditampilkan dari PostgreSQL.")


# =========================================================
# Header
# =========================================================
st.markdown(
    """
    <div class="hero">
        <div class="hero-eyebrow">DATA ENGINEERING · MONITORING</div>
        <h1>📡 E-Commerce Stream Monitor</h1>
        <p>
            Pantau event transaksi, kualitas data, aktivitas pipeline,
            dan ringkasan penjualan dari Kafka ke PostgreSQL.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

st.caption(
    "ℹ️ Dataset sumber bersifat historis dan dikirim bertahap untuk simulasi streaming; "
    "bukan transaksi e-commerce live."
)


# =========================================================
# Pengambilan data dan tampilan
# =========================================================
try:
    kpi = read_query(
        """
        SELECT
            (SELECT COUNT(*) FROM raw_events) AS total_events,
            (SELECT COUNT(*) FROM dead_letter_events) AS failed_events,
            COALESCE(
                (SELECT SUM(total_quantity)
                 FROM v_retail_sales_summary), 0
            ) AS total_quantity,
            COALESCE(
                (SELECT SUM(estimated_gross_value)
                 FROM v_retail_sales_summary), 0
            ) AS estimated_gross_value,
            COALESCE(
                (SELECT SUM(total_countries)
                 FROM v_retail_sales_summary), 0
            ) AS total_countries
        """
    )

    values = kpi.iloc[0]
    total_events = int(values["total_events"])
    failed_events = int(values["failed_events"])
    total_quantity = int(values["total_quantity"])
    gross_value = float(values["estimated_gross_value"])
    total_countries = int(values["total_countries"])

    total_records = total_events + failed_events
    success_rate = total_events / total_records * 100 if total_records else 0
    dlq_rate = failed_events / total_records * 100 if total_records else 0

    # -----------------------------------------------------
    # OVERVIEW
    # -----------------------------------------------------
    if page == "🏠 Overview":
        st.subheader("📌 Ringkasan utama")
        st.markdown(
            '<div class="section-subtitle">Kondisi data berdasarkan catatan database saat ini.</div>',
            unsafe_allow_html=True,
        )

        c1, c2, c3, c4 = st.columns(4)
        c1.metric(
            "📥 Event tersimpan",
            format_integer(total_events),
            help="Jumlah baris yang tersimpan di tabel raw_events.",
        )
        c2.metric(
            "🚨 Event gagal · DLQ",
            format_integer(failed_events),
            help="Jumlah baris yang tersimpan di dead_letter_events.",
        )
        c3.metric(
            "📦 Total kuantitas",
            format_integer(total_quantity),
            help="Total kuantitas berdasarkan view analitik.",
        )
        c4.metric(
            "💰 Estimasi penjualan",
            format_money(gross_value),
            help="Estimasi gross value dari view analitik, bukan laporan keuangan.",
        )

        st.markdown("### 🧪 Kualitas data")
        st.markdown(
            '<div class="section-subtitle">Rasio berdasarkan jumlah catatan pada kedua tabel.</div>',
            unsafe_allow_html=True,
        )
        q1, q2, q3 = st.columns(3)
        q1.metric("🗂️ Total catatan event", format_integer(total_records))
        q2.metric("✅ Rasio tersimpan", f"{success_rate:.2f}%")
        q3.metric("⚠️ Rasio DLQ", f"{dlq_rate:.2f}%")

        st.caption(
            "Rasio ini membandingkan catatan raw_events dan dead_letter_events. "
            "Nilainya bukan tingkat keberhasilan transaksi unik karena kedua tabel "
            "mencatat event yang berbeda."
        )

        st.divider()

        # Grafik aktivitas dan status penyimpanan
        left, right = st.columns([1.55, 1])

        with left:
            st.markdown("### 📈 Aktivitas event")
            st.caption("Jumlah event yang diterima PostgreSQL per jam.")

            trend = read_query(
                """
                SELECT
                    date_trunc('hour', received_at) AS event_hour,
                    COUNT(*) AS total_events
                FROM raw_events
                GROUP BY date_trunc('hour', received_at)
                ORDER BY event_hour
                """
            )

            if trend.empty:
                st.info("📭 Belum ada event untuk divisualisasikan.")
            else:
                trend["event_hour"] = pd.to_datetime(trend["event_hour"])
                trend = trend.set_index("event_hour")
                st.line_chart(
                    trend["total_events"],
                    use_container_width=True,
                )

        with right:
            st.markdown("### 🧭 Status penyimpanan")
            st.caption("Perbandingan catatan tersimpan dan gagal.")

            status_df = pd.DataFrame(
                {
                    "Status": ["Tersimpan", "Gagal · DLQ"],
                    "Jumlah": [total_events, failed_events],
                }
            )

            st.bar_chart(
                status_df.set_index("Status"),
                use_container_width=True,
            )

            if failed_events > 0:
                st.warning(
                    f"🚨 Ada {failed_events:,} catatan di DLQ. "
                    "Periksa alasan kegagalannya sebelum melakukan replay."
                )
            else:
                st.success("✅ Belum ada catatan gagal di DLQ.")

        st.divider()

        # Analisis alasan kegagalan
        st.markdown("### 🛠️ Analisis kegagalan event")
        st.markdown(
            '<div class="section-subtitle">Alasan kegagalan yang tercatat di Dead Letter Queue.</div>',
            unsafe_allow_html=True,
        )

        dlq_reasons = read_query(
            """
            SELECT
                error_reason,
                COUNT(*) AS total_events
            FROM dead_letter_events
            GROUP BY error_reason
            ORDER BY total_events DESC
            """
        )

        if dlq_reasons.empty:
            st.success("✅ Belum ada kegagalan yang tercatat.")
        else:
            reason_chart, reason_table = st.columns([1.2, 1])

            with reason_chart:
                st.bar_chart(
                    dlq_reasons.set_index("error_reason")["total_events"],
                    use_container_width=True,
                )

            with reason_table:
                st.dataframe(
                    dlq_reasons.rename(
                        columns={
                            "error_reason": "Alasan kegagalan",
                            "total_events": "Jumlah event",
                        }
                    ),
                    use_container_width=True,
                    hide_index=True,
                )

        # Distribusi sumber event
        st.markdown("### 🛰️ Distribusi sumber event")
        source_summary = read_query(
            """
            SELECT
                source,
                COUNT(*) AS total_events
            FROM raw_events
            GROUP BY source
            ORDER BY total_events DESC
            """
        )

        if source_summary.empty:
            st.info("📭 Belum ada data sumber event.")
        else:
            st.bar_chart(
                source_summary.set_index("source")["total_events"],
                use_container_width=True,
            )

        st.divider()

        # Ringkasan view analitik
        st.markdown("### 🛒 Ringkasan penjualan")
        summary = read_query(
            """
            SELECT
                total_events,
                total_quantity,
                estimated_gross_value,
                total_countries,
                first_invoice_date,
                last_invoice_date
            FROM v_retail_sales_summary
            """
        )

        if summary.empty:
            st.info("📭 View analitik belum memiliki data.")
        else:
            summary = summary.rename(
                columns={
                    "total_events": "Event analitik",
                    "total_quantity": "Total kuantitas",
                    "estimated_gross_value": "Estimasi gross value",
                    "total_countries": "Jumlah negara",
                    "first_invoice_date": "Invoice pertama",
                    "last_invoice_date": "Invoice terakhir",
                }
            )
            st.dataframe(
                summary,
                use_container_width=True,
                hide_index=True,
            )

        # Transaksi terbaru
        st.markdown("### 🧾 Transaksi terbaru")
        recent = read_query(
            """
            SELECT
                id,
                event_id,
                event_type,
                source,
                payload->'data'->>'Invoice' AS invoice,
                payload->'data'->>'StockCode' AS stock_code,
                payload->'data'->>'Quantity' AS quantity,
                payload->'data'->>'Price' AS price,
                payload->'data'->>'Country' AS country,
                received_at
            FROM raw_events
            ORDER BY received_at DESC, id DESC
            LIMIT 10
            """
        )

        if recent.empty:
            st.info("📭 Belum ada transaksi tersimpan.")
        else:
            st.dataframe(
                recent,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "id": st.column_config.NumberColumn("ID", format="%d"),
                    "event_id": st.column_config.NumberColumn("Event ID"),
                    "event_type": st.column_config.TextColumn("Tipe event"),
                    "source": st.column_config.TextColumn("Sumber"),
                    "invoice": st.column_config.TextColumn("Invoice"),
                    "stock_code": st.column_config.TextColumn("Stock code"),
                    "quantity": st.column_config.TextColumn("Kuantitas"),
                    "price": st.column_config.TextColumn("Harga"),
                    "country": st.column_config.TextColumn("Negara"),
                    "received_at": st.column_config.DatetimeColumn(
                        "Waktu diterima",
                        format="YYYY-MM-DD HH:mm:ss",
                    ),
                },
            )

    # -----------------------------------------------------
    # TRANSAKSI
    # -----------------------------------------------------
    elif page == "🧾 Transaksi":
        st.subheader("🧾 Data transaksi")
        st.markdown(
            '<div class="section-subtitle">Event yang tersimpan di raw_events. Maksimal 500 baris ditampilkan.</div>',
            unsafe_allow_html=True,
        )

        transactions = read_query(
            """
            SELECT
                id,
                event_id,
                event_type,
                source,
                kafka_partition,
                kafka_offset,
                payload->'data'->>'Invoice' AS invoice,
                payload->'data'->>'StockCode' AS stock_code,
                payload->'data'->>'Quantity' AS quantity,
                payload->'data'->>'Price' AS price,
                payload->'data'->>'Country' AS country,
                received_at
            FROM raw_events
            ORDER BY received_at DESC, id DESC
            LIMIT 500
            """
        )

        search = st.text_input(
            "🔎 Cari invoice, stock code, atau negara",
            placeholder="Contoh: United Kingdom",
        )

        if search and not transactions.empty:
            searchable = (
                transactions[["invoice", "stock_code", "country"]]
                .fillna("")
                .astype(str)
                .agg(" ".join, axis=1)
            )
            transactions = transactions[
                searchable.str.contains(search, case=False, regex=False)
            ]

        st.caption(f"📄 {len(transactions):,} baris ditampilkan.")
        st.dataframe(
            transactions,
            use_container_width=True,
            hide_index=True,
        )

        if not transactions.empty:
            st.download_button(
                "⬇️ Unduh CSV transaksi",
                data=transactions.to_csv(index=False).encode("utf-8"),
                file_name="ecommerce_transactions.csv",
                mime="text/csv",
                use_container_width=False,
            )

    # -----------------------------------------------------
    # DEAD LETTER QUEUE
    # -----------------------------------------------------
    else:
        st.subheader("🚨 Dead Letter Queue")
        st.markdown(
            '<div class="section-subtitle">Event yang gagal melewati validasi atau proses penyimpanan.</div>',
            unsafe_allow_html=True,
        )

        dlq = read_query(
            """
            SELECT
                id,
                event_id,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                error_reason,
                payload,
                failed_at,
                retry_count
            FROM dead_letter_events
            ORDER BY failed_at DESC, id DESC
            LIMIT 500
            """
        )

        if dlq.empty:
            st.success("✅ Tidak ada event di Dead Letter Queue.")
        else:
            reasons = ["Semua alasan"] + sorted(
                dlq["error_reason"].dropna().unique().tolist()
            )
            selected_reason = st.selectbox(
                "🔎 Filter alasan kegagalan",
                reasons,
            )

            if selected_reason != "Semua alasan":
                dlq = dlq[dlq["error_reason"] == selected_reason]

            st.caption(f"📄 {len(dlq):,} event ditampilkan.")
            st.dataframe(
                dlq.drop(columns=["payload"]),
                use_container_width=True,
                hide_index=True,
            )

            with st.expander("🔍 Lihat payload event gagal"):
                st.dataframe(
                    dlq[["event_id", "error_reason", "payload"]],
                    use_container_width=True,
                    hide_index=True,
                )

            st.download_button(
                "⬇️ Unduh CSV DLQ",
                data=dlq.drop(columns=["payload"]).to_csv(index=False).encode("utf-8"),
                file_name="ecommerce_dead_letter_events.csv",
                mime="text/csv",
            )

    st.divider()
    st.markdown(
        '<div class="footnote">📡 E-Commerce Stream Monitor · PostgreSQL · Read-only dashboard</div>',
        unsafe_allow_html=True,
    )

except Exception as exc:
    st.error("⚠️ Dashboard gagal mengambil data dari PostgreSQL.")
    st.code(f"{type(exc).__name__}: {exc}")
    st.info(
        "Pastikan Docker Compose aktif, konfigurasi .env benar, dan tabel "
        "raw_events, dead_letter_events, serta view v_retail_sales_summary tersedia."
    )
