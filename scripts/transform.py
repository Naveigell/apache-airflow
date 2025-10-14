import os
from datetime import datetime

import pandas as pd

from dotenv import load_dotenv

from scripts import utils

load_dotenv()

def transform_data(df_books, df_patrons, df_loans):
    """Melakukan join, pembersihan, dan penambahan kolom baru."""
    print("2. Transforming data...")

    # 2.1 Join: Menggabungkan Loans dengan Books dan Patrons

    # Gabung Loans dengan Books menggunakan book_id
    df_transformed = pd.merge(df_loans, df_books[['book_id', 'title', 'genre', 'author']],
                              on='book_id', how='left')

    # Gabung hasil dengan Patrons menggunakan patron_id
    df_transformed = pd.merge(df_transformed, df_patrons[['patron_id', 'name', 'membership_date']],
                              on='patron_id', how='left')

    # 2.2 Konversi Tipe Data dan Perhitungan Waktu

    # Kolom waktu
    date_cols = ['loan_timestamp', 'due_date', 'return_date']
    for col in date_cols:
        # Konversi ke datetime. loan_timestamp adalah datetime, lainnya adalah date.
        if col == 'loan_timestamp':
            df_transformed[col] = pd.to_datetime(df_transformed[col])
        else:
            df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')

    # Hitung Durasi Peminjaman (Duration_Days)
    # Jika buku sudah dikembalikan (return_date ada), hitung durasi dari loan_timestamp ke return_date
    # Jika buku masih dipinjam, hitung durasi hingga hari ini
    now = datetime.now()
    df_transformed['actual_return_date'] = df_transformed['return_date'].fillna(now)

    df_transformed['duration_days'] = (
            df_transformed['actual_return_date'] - df_transformed['loan_timestamp']
    ).dt.days

    # Hitung Keterlambatan (Is_Overdue)
    # Bandingkan return_date dengan due_date (hanya untuk yang sudah dikembalikan)
    # Untuk yang statusnya 'On Loan' atau 'Overdue' (belum kembali), gunakan tanggal hari ini

    df_transformed['return_check_date'] = df_transformed['return_date'].fillna(now)

    # Kolom Is_Late: True jika tanggal kembali > tanggal jatuh tempo
    df_transformed['is_late'] = (
            df_transformed['return_check_date'] > df_transformed['due_date']
    )

    # Hapus kolom bantu
    df_transformed = df_transformed.drop(columns=['actual_return_date', 'return_check_date'])

    # 2.3 Pembersihan Akhir dan Pemilihan Kolom

    # Ganti nama kolom untuk kejelasan
    df_transformed.rename(columns={
        'name': 'patron_name',
        'title': 'book_title'
    }, inplace=True)

    # Pilih dan susun ulang kolom yang relevan untuk analisis
    final_cols = [
        'loan_id', 'book_id', 'patron_id',
        'patron_name', 'membership_date',
        'book_title', 'genre', 'author',
        'loan_timestamp', 'due_date', 'return_date',
        'status', 'duration_days', 'is_late'
    ]
    df_transformed = df_transformed[final_cols]

    print("   -> Transformation successful. Created 'duration_days' and 'is_late' columns.")
    return df_transformed


