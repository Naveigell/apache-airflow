import sqlite3
import random
import os
from faker import Faker
from datetime import datetime, timedelta

from dotenv import load_dotenv

load_dotenv()

# --- Konfigurasi ---
DB_NAME = os.path.dirname(os.path.abspath(__file__)) + '/../data/raw/' + os.getenv('DATABASE_NAME')


# Jumlah Record (Total Records: 500 + 500 + 1500 = 2500 records, > 2000)
NUM_BOOKS = 500
NUM_PATRONS = 500
NUM_LOANS = 1500

# Batas tanggal
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime.now()

# Inisialisasi Faker
try:
    fake = Faker('id_ID')
except ImportError:
    fake = Faker('en_US')

book_genres = ['Fiction', 'Science', 'History', 'Technology', 'Biography', 'Literature', 'Health', 'Education', 'Art',
               'Travel']


# --- 2. Fungsi Generator Data ---

def generate_books(num):
    """Menghasilkan data untuk tabel 'books' dengan snake_case."""
    print("  -> Generating books data...")
    books = []
    for i in range(1, num + 1):
        books.append((
            f"B{i:05d}",  # book_id
            fake.sentence(nb_words=5).strip('.'),  # title
            fake.name(),  # author
            fake.isbn13(),  # isbn
            random.choice(book_genres),  # genre
            fake.date_between(start_date='-10y', end_date='today').strftime('%Y-%m-%d'),  # publication_date
            random.randint(1, 5)  # total_copies
        ))
    return books


def generate_patrons(num):
    """Menghasilkan data untuk tabel 'patrons' dengan snake_case."""
    print("  -> Generating patrons data...")
    patrons = []
    for i in range(1, num + 1):
        patrons.append((
            f"P{i:05d}",  # patron_id
            fake.name(),  # name
            fake.address().replace('\n', ', '),  # address
            fake.email(),  # email
            fake.phone_number(),  # phone
            fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')  # membership_date
        ))
    return patrons


def generate_loans(num, book_ids, patron_ids):
    """Menghasilkan data untuk tabel 'loans' dengan snake_case."""
    print("  -> Generating loans data...")
    loans = []
    for i in range(1, num + 1):
        loan_id = f"L{i:06d}"
        book_id = random.choice(book_ids)
        patron_id = random.choice(patron_ids)

        loan_date = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
        due_date = loan_date + timedelta(days=random.randint(7, 30))

        # Logika Pengembalian: 80% Returned
        if random.random() < 0.8:
            return_days = random.randint(1, 14) if random.random() < 0.9 else random.randint(15, 60)
            return_date = loan_date + timedelta(days=return_days)
            return_date = min(return_date, END_DATE)
            status = "Returned"
        else:
            return_date = None
            status = "On Loan" if due_date > END_DATE else "Overdue"

        loans.append((
            loan_id,  # loan_id
            book_id,  # book_id (Foreign Key)
            patron_id,  # patron_id (Foreign Key)
            loan_date.strftime('%Y-%m-%d %H:%M:%S'),  # loan_timestamp
            due_date.strftime('%Y-%m-%d'),  # due_date
            return_date.strftime('%Y-%m-%d') if return_date else None,  # return_date
            status  # status
        ))
    return loans


# --- 3. Fungsi Database SQLite ---

def create_schema_and_insert(conn, cursor, book_data, patron_data, loan_data):
    """Membuat skema dengan snake_case dan memasukkan data."""

    cursor.execute("PRAGMA foreign_keys = ON;")

    # --- Skema Tabel ---
    # Tabel: patrons
    cursor.execute("""
                   CREATE TABLE patrons
                   (
                       patron_id       TEXT PRIMARY KEY,
                       name            TEXT NOT NULL,
                       address         TEXT,
                       email           TEXT UNIQUE,
                       phone           TEXT,
                       membership_date DATE NOT NULL
                   );
                   """)

    # Tabel: books
    cursor.execute("""
                   CREATE TABLE books
                   (
                       book_id          TEXT PRIMARY KEY,
                       title            TEXT    NOT NULL,
                       author           TEXT    NOT NULL,
                       isbn             TEXT UNIQUE,
                       genre            TEXT,
                       publication_date DATE,
                       total_copies     INTEGER NOT NULL DEFAULT 1
                   );
                   """)

    # Tabel: loans
    cursor.execute("""
                   CREATE TABLE loans
                   (
                       loan_id        TEXT PRIMARY KEY,
                       book_id        TEXT     NOT NULL,
                       patron_id      TEXT     NOT NULL,
                       loan_timestamp DATETIME NOT NULL,
                       due_date       DATE     NOT NULL,
                       return_date    DATE,
                       status         TEXT     NOT NULL,

                       FOREIGN KEY (book_id) REFERENCES books (book_id),
                       FOREIGN KEY (patron_id) REFERENCES patrons (patron_id)
                   );
                   """)
    print("  -> Database schema (using snake_case) created successfully.")

    # --- Insertion Data ---

    # 1. Insert Patrons
    cursor.executemany("INSERT INTO patrons VALUES (?, ?, ?, ?, ?, ?)", patron_data)
    print(f"  -> Inserted {len(patron_data)} records into patrons.")

    # 2. Insert Books
    cursor.executemany("INSERT INTO books VALUES (?, ?, ?, ?, ?, ?, ?)", book_data)
    print(f"  -> Inserted {len(book_data)} records into books.")

    # 3. Insert Loans
    cursor.executemany("INSERT INTO loans VALUES (?, ?, ?, ?, ?, ?, ?)", loan_data)
    print(f"  -> Inserted {len(loan_data)} records into loans.")

    conn.commit()


# --- 4. Main Execution ---

if __name__ == '__main__':
    # Hapus file database lama jika ada
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)

        print(f"Removed existing database file: {DB_NAME}")
    else:
        os.makedirs(os.path.dirname(DB_NAME), exist_ok=True)

        print(f"Created new database file: {DB_NAME}")

    print("Synthetic data generation success.")

    # 1. Generate Data in Memory
    books = generate_books(NUM_BOOKS)
    patrons = generate_patrons(NUM_PATRONS)

    book_ids = [b[0] for b in books]
    patron_ids = [p[0] for p in patrons]

    loans = generate_loans(NUM_LOANS, book_ids, patron_ids)

    # 2. Connect and Insert
    print("\n--- Connecting to SQLite and Inserting Data ---")
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        print(f"  -> Database file created: {DB_NAME}")

        create_schema_and_insert(conn, cursor, books, patrons, loans)

        conn.close()

        total_records = len(books) + len(patrons) + len(loans)
        print("\n--- PROCESS COMPLETE ---")
        print(f"Total Records Generated and Inserted: {total_records}")
        print(f"Target Records (>2000) Achieved: {total_records >= 2000}")
        print(f"Data sudah dimasukkan langsung ke {DB_NAME} dengan nama kolom snake_case.")

    except Exception as e:
        print(f"\nFATAL ERROR during database operation: {e}")