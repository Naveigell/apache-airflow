import pandas as pd


def run_data_quality_checks(df, table_name):
    errors = []

    critical_cols = {
        'books': ['book_id', 'title', 'author', 'total_copies'],
        'patrons': ['patron_id', 'name', 'membership_date'],
        'loans': ['loan_id', 'book_id', 'patron_id', 'loan_timestamp', 'due_date', 'status']
    }.get(table_name, [])

    # check every column that have NULL value
    for col in critical_cols:
        if df[col].isnull().any():
            errors.append(f"There is NULL values in '{col}' column.")

    id_col = f'{table_name[:-1]}_id' if table_name != 'books' else 'book_id'

    # the primary key should be string or object
    if id_col in df.columns:
        if not pd.api.types.is_string_dtype(df[id_col].dtype):
            if pd.api.types.is_numeric_dtype(df[id_col].dtype):
                errors.append(f"Primary key in '{id_col}' should be string or object but got numeric.")

    # check if column has duplicate value
    if id_col in df.columns:
        if df[id_col].duplicated().any():
            errors.append(f"Duplicate primary key in '{id_col}'.")

    # check if total_copies is negative
    if 'total_copies' in df.columns:
        if (df['total_copies'] < 0).any():
            errors.append("The total copies could not lower than 0")

    # check if status is in valid statuses
    if 'status' in df.columns:
        valid_statuses = ['Returned', 'On Loan', 'Overdue']

        if not df['status'].isin(valid_statuses).all():
            errors.append("There is invalid status in 'status' column.")

    if errors:
        print("\n--- DATA QUALITY CHECK FAILED! Stopping Pipeline ---")
        for error in errors:
            print(f"- {error}")
        # Stop Pipeline: Raise exception
        raise ValueError(f"Data Quality Check FAILED for table {table_name}. Found {len(errors)} issues.")

    print(f"Data Quality Check for {table_name} PASSED. ({len(df)} rows)")

    return True


def perform_data_cleaning(df_books, df_patrons, df_loans):
    """
    Task 7: Handle missing values, remove duplicates, and standardize formats.
    """
    print("\n--- Starting Data Cleaning (Task 7) ---")

    # 1. Remove Duplicates (sesuai Primary Key)
    df_books.drop_duplicates(subset=['book_id'], inplace=True)
    df_patrons.drop_duplicates(subset=['patron_id'], inplace=True)
    df_loans.drop_duplicates(subset=['loan_id'], inplace=True)
    print("-> Duplicates on Primary Keys removed.")

    # 2. Handle Missing Values (Mengisi atau menghapus NaN di kolom kunci)

    # Menghapus baris pinjaman yang memiliki NULL pada critical columns (kecuali return_date)
    df_loans.dropna(subset=['loan_id', 'book_id', 'patron_id', 'loan_timestamp', 'due_date'], inplace=True)
    print("-> Missing values in critical loan columns removed.")

    # 3. Standardize formats and Fix Data Type Issues (Date/Time Conversions)

    # Books: Konversi ke tipe data Date, mengubah nilai buruk (error) menjadi NaT/None
    df_books['publication_date'] = pd.to_datetime(df_books['publication_date'], errors='coerce').dt.date

    # Patrons
    df_patrons['membership_date'] = pd.to_datetime(df_patrons['membership_date'], errors='coerce').dt.date

    # Loans
    df_loans['loan_timestamp'] = pd.to_datetime(df_loans['loan_timestamp'], errors='coerce')
    df_loans['due_date'] = pd.to_datetime(df_loans['due_date'], errors='coerce').dt.date
    # return_date bisa NULL, biarkan NaT jika konversi gagal
    df_loans['return_date'] = pd.to_datetime(df_loans['return_date'], errors='coerce').dt.date

    # Standardize Text (Membersihkan spasi di awal/akhir)
    df_books['title'] = df_books['title'].astype(str).str.strip()
    df_patrons['name'] = df_patrons['name'].astype(str).str.strip()

    # 4. Referential Integrity Check (External FK check)
    # Menghapus catatan loans yang merujuk pada book_id atau patron_id yang tidak ada di dimensi.
    valid_book_ids = df_books['book_id'].unique()
    valid_patron_ids = df_patrons['patron_id'].unique()

    initial_loan_count = len(df_loans)
    df_loans = df_loans[df_loans['book_id'].isin(valid_book_ids)]
    df_loans = df_loans[df_loans['patron_id'].isin(valid_patron_ids)]

    dropped_loans = initial_loan_count - len(df_loans)
    if dropped_loans > 0:
        print(f"-> Removed {dropped_loans} 'loans' rows due to failed referential integrity.")

    print("Data Cleaning Finished.")

    return df_books, df_patrons, df_loans