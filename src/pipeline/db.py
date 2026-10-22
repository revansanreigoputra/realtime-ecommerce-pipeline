import os

import psycopg
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    """Membuka koneksi ke PostgreSQL."""
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
        dbname=os.getenv("POSTGRES_DB", "ecommerce_stream"),
        user=os.getenv("POSTGRES_USER", "pipeline_user"),
        password=os.environ["POSTGRES_PASSWORD"],
    )
