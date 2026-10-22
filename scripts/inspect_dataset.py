import argparse
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET = PROJECT_ROOT / "data" / "raw" / "online_retail_II.xlsx"
DEFAULT_SHEET = "Year 2009-2010"


def main():
    parser = argparse.ArgumentParser(
        description="Memeriksa dataset Excel Online Retail II."
    )
    parser.add_argument(
        "--file",
        type=Path,
        default=DEFAULT_DATASET,
        help="Lokasi file Excel.",
    )
    parser.add_argument(
        "--sheet",
        default=DEFAULT_SHEET,
        help="Nama sheet yang akan diperiksa.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Jumlah baris yang dibaca untuk pemeriksaan.",
    )
    args = parser.parse_args()

    if not args.file.exists():
        raise FileNotFoundError(
            f"Dataset tidak ditemukan: {args.file}"
        )

    if args.rows < 1:
        parser.error("--rows harus minimal 1.")

    print(f"File: {args.file}")

    excel = pd.ExcelFile(args.file)
    print(f"Sheet tersedia: {excel.sheet_names}")

    if args.sheet not in excel.sheet_names:
        raise ValueError(
            f"Sheet '{args.sheet}' tidak ditemukan. "
            f"Pilihan: {excel.sheet_names}"
        )

    df = pd.read_excel(
        args.file,
        sheet_name=args.sheet,
        nrows=args.rows,
    )

    df.columns = [
        str(column).strip().replace(" ", "_")
        for column in df.columns
    ]

    print(f"\nSheet diperiksa: {args.sheet}")
    print(f"Jumlah baris dibaca: {len(df)}")
    print(f"Jumlah kolom: {len(df.columns)}")

    print("\nDaftar kolom:")
    for column in df.columns:
        print(f"- {column}")

    print("\nNilai kosong per kolom:")
    missing = df.isna().sum()
    print(missing[missing > 0].to_string() or "Tidak ada nilai kosong.")

    print("\nJumlah baris duplikat dalam sampel:")
    print(df.duplicated().sum())

    print("\nContoh 5 baris:")
    print(df.head().to_string(index=False))


if __name__ == "__main__":
    main()
