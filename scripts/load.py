# scripts/load.py
import os

import extract as extract
import transform as transform

RAW_FOLDER = os.path.dirname(os.path.abspath(__file__)) + '/../DATA/' + os.getenv('RAW_FOLDER')
OUTPUT_FILE = os.path.join(RAW_FOLDER, 'library_transformed_data.csv')

DB_NAME = os.path.dirname(os.path.abspath(__file__)) + '/../databases/' + os.getenv('DATABASE_NAME')

def load_data(df, output_file):
    """Menyimpan DataFrame hasil transformasi ke file CSV di folder raw."""
    # Buat folder 'raw' jika belum ada
    if not os.path.exists(RAW_FOLDER):
        os.makedirs(RAW_FOLDER)
        print(f"3. Created directory: {RAW_FOLDER}")

    df.to_csv(output_file, index=False)
    print(f"   -> Data loaded successfully into CSV: {output_file}")
    print(f"   -> Total records loaded: {len(df)}")

[df_books, df_patrons, df_loans] = [
    extract.extract_data_from_source(DB_NAME, 'books'),
    extract.extract_data_from_source(DB_NAME, 'patrons'),
    extract.extract_data_from_source(DB_NAME, 'loans')
]

transformed = transform.transform_data(df_books, df_patrons, df_loans)

load_data(transformed, OUTPUT_FILE)
