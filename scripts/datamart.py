import pandas as pd
import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()

# --- Konfigurasi Folder ---
# Variabel lingkungan sudah dimuat sebelumnya di DAG

FOLDER              = os.path.dirname(os.path.abspath(__file__)) + '/../data/'
STAGING_DATABASE    = FOLDER + 'staging/' + os.getenv('STAGING_DATABASE')
DATAMART_DATABASE   = FOLDER + 'datamart/' + os.getenv('DATAMART_DATABASE')
DATAMART_TABLE_NAME = 'staging_data'


def get_db_connection(db_path):
    """Membuka koneksi SQLite."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(db_path)


def create_dim_date(conn):
    """
    Task 8: Membuat Dim_Date (Wajib) dari tanggal yang ada di Staging.
    Membuat rentang 365 hari ke depan dari tanggal hari ini untuk completeness.
    """
    print("-> Creating Dim_Date...")

    # Ambil semua tanggal unik (loan, due, return, membership, publication) dari Staging
    conn_staging = get_db_connection(STAGING_DATABASE)
    query = f"SELECT loan_timestamp, due_date, return_date, membership_date FROM {DATAMART_TABLE_NAME}"
    df_staging = pd.read_sql(query, conn_staging)
    conn_staging.close()

    # Kumpulkan semua tanggal unik dan ubah ke format YYYY-MM-DD
    all_dates = pd.concat([
        pd.to_datetime(df_staging['loan_timestamp']).dt.normalize(),
        pd.to_datetime(df_staging['due_date']),
        pd.to_datetime(df_staging['return_date']),
        pd.to_datetime(df_staging['membership_date']),
    ]).dropna().unique()

    # Tambahkan rentang tanggal dari tanggal paling awal hingga 1 tahun dari sekarang
    min_date = pd.to_datetime(all_dates).min() if len(all_dates) > 0 else pd.Timestamp('2023-01-01')
    max_date = pd.Timestamp.today() + pd.DateOffset(years=1)

    date_range = pd.date_range(start=min_date, end=max_date, freq='D')

    df_date = pd.DataFrame({'date': date_range})

    # Membuat kolom Dimensi Waktu
    df_date['date_key'] = (df_date['date'].dt.strftime('%Y%m%d')).astype(int)
    df_date['full_date'] = df_date['date'].dt.date
    df_date['year'] = df_date['date'].dt.year
    df_date['quarter'] = df_date['date'].dt.quarter
    df_date['month'] = df_date['date'].dt.month
    df_date['day'] = df_date['date'].dt.day
    df_date['day_name'] = df_date['date'].dt.day_name()
    df_date['is_weekend'] = (df_date['date'].dt.dayofweek >= 5).astype(int)

    # Pilih kolom akhir
    df_date = df_date[['date_key', 'full_date', 'year', 'quarter', 'month', 'day', 'day_name', 'is_weekend']]

    # Load ke Datamart
    df_date.to_sql('Dim_Date', conn, if_exists='replace', index=False)
    print(f"-> Dim_Date created with {len(df_date)} records.")
    return df_date[['date_key', 'full_date']]  # Hanya kembalikan subset yang diperlukan untuk FK


def create_dim_book_patron(conn, df_staging):
    """
    Task 8: Membuat Dim_Book dan Dim_Patron (SCD Type 1).
    """

    # ------------------ DIM_BOOK ------------------
    print("-> Creating Dim_Book (SCD Type 1)...")
    df_book = df_staging[['book_id', 'title', 'genre', 'total_copies']].drop_duplicates().reset_index(drop=True)

    # Membuat Surrogate Key (dihitung dari index + 1)
    df_book['book_key'] = df_book.index + 1

    # Pembersihan nama kolom dan penempatan kunci
    df_book = df_book[['book_key', 'book_id', 'title', 'genre', 'total_copies']]
    df_book.to_sql('Dim_Book', conn, if_exists='replace', index=False)
    print(f"-> Dim_Book created with {len(df_book)} records.")

    # ------------------ DIM_PATRON ------------------
    print("-> Creating Dim_Patron (SCD Type 1)...")
    df_patron = df_staging[['patron_id', 'name', 'membership_date']].drop_duplicates().reset_index(drop=True)

    # Membuat Surrogate Key
    df_patron['patron_key'] = df_patron.index + 1

    # Pembersihan nama kolom dan penempatan kunci
    df_patron = df_patron[['patron_key', 'patron_id', 'name', 'membership_date']]
    df_patron.to_sql('Dim_Patron', conn, if_exists='replace', index=False)
    print(f"-> Dim_Patron created with {len(df_patron)} records.")

    # Mengembalikan DataFrames dimensi untuk Task 9
    return df_book[['book_key', 'book_id']], df_patron[['patron_key', 'patron_id']]


def datamart_build_pipeline():
    """
    Mengkoordinasikan Task 8 (Dimensi) dan Task 9 (Fakta).
    Dipanggil oleh PythonOperator di DAG.
    """
    print("\n==============================================")
    print("STAGE 3: Starting Dimensional Modeling (Task 8 & 9)")
    print("==============================================")

    # 1. Ambil data staging yang bersih
    conn_staging = get_db_connection(STAGING_DATABASE)
    query = f"SELECT * FROM {DATAMART_TABLE_NAME}"
    df_staging = pd.read_sql(query, conn_staging)
    conn_staging.close()

    if df_staging.empty:
        print("Staging data is empty. Skipping Data Mart build.")
        return

    # 2. Buka koneksi ke Datamart
    conn_datamart = get_db_connection(DATAMART_DATABASE)

    # --- Task 8: CREATE DIMENSION TABLES ---

    # Dim_Date
    df_dim_date = create_dim_date(conn_datamart)

    # Dim_Book dan Dim_Patron
    df_dim_book, df_dim_patron = create_dim_book_patron(conn_datamart, df_staging)

    # --- Task 9: CREATE FACT TABLE ---
    # (Logika Task 9 akan ditambahkan di sini pada langkah berikutnya)

    conn_datamart.close()
    print("Dimensional Model Build (Task 8) COMPLETE.")

# Task 9 create_fact_table akan ditambahkan di sini.
