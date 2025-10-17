import sqlite3
import pandas as pd
import os

BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
DATA_FOLDER      = os.path.join(BASE_DIR, '..', 'data')
DATAMART_DB_PATH = os.path.join(DATA_FOLDER, 'datamart', 'datamart.sqlite')

QUERIES = {
    "Buku dengan peminjaman tertinggi": """
                                                        SELECT T1.book_title       AS "Judul Buku",
                                                               SUM(T2.total_loans) AS "Total Peminjaman"
                                                        FROM Dim_Book AS T1
                                                                 JOIN Loan_Performance_Mart AS T2
                                                                      ON T1.book_key = T2.book_key
                                                        GROUP BY 1
                                                        ORDER BY "Total Peminjaman" DESC LIMIT 5;
                                                        """,
    "Tren kinerja dengan sistem bulanan": """
                                                              SELECT CAST(T1.year AS INTEGER)              AS Tahun,
                                                                     CAST(T1.month AS INTEGER)             AS Bulan,
                                                                     T1.monthly_loan_volume                AS "Volume Peminjaman Bulanan",
                                                                  
                                                                     ROUND(T1.overdue_rate_kpi * 100, 2)   AS "Overdue Rate (%)",
                                                                     ROUND(T1.avg_loan_duration_system, 1) AS "Rata-rata Durasi Pinjaman (Hari)"
                                                              FROM Monthly_KPI_Summary AS T1
                                                              ORDER BY Tahun, Bulan;
                                                              """,
    "Perbandingan pola peminjaman anggota baru vs yang lama": """
                                                             SELECT CASE
                                                                        WHEN (julianday('now') - julianday(T2.membership_date)) < 180
                                                                            THEN 'Anggota Baru (< 6 Bln)'
                                                                        ELSE 'Anggota Lama'
                                                                        END                       AS "Anggota",
                                                                    COUNT(T1.loan_id)             AS "Transaksi",
                                                                    ROUND(AVG(T1.days_loaned), 1) AS "Rata-rata Durasi Pinjaman per Hari"
                                                             FROM Fact_Loan_Transactions AS T1
                                                                      JOIN Dim_Patron AS T2
                                                                           ON T1.patron_key = T2.patron_key
                                                             GROUP BY 1;
                                                             """,
    "Distribusi peminjaman berdasarkan genre": """
                                                  SELECT T2.genre                      AS "Genre Buku",
                                                         SUM(T1.loan_count)            AS "Total Peminjaman",
                                                         ROUND(AVG(T1.days_loaned), 1) AS "Rata-rata Durasi per Hari"
                                                  FROM Fact_Loan_Transactions AS T1
                                                           JOIN Dim_Book AS T2
                                                                ON T1.book_key = T2.book_key
                                                  GROUP BY 1
                                                  ORDER BY "Total Peminjaman" DESC;
                                                  """,
    "Pemanfaatan peminjaman hari libur": """
                                                          SELECT CASE T2.is_weekend
                                                                     WHEN 1 THEN 'Akhir Pekan'
                                                                     ELSE 'Hari Kerja' END      AS "Jenis Hari",
                                                                 SUM(T1.monthly_loan_volume)                  AS "Total Volume Pinjaman",
                                                                 ROUND(SUM(T1.monthly_loan_volume) * 1.0 /
                                                                       (SELECT COUNT(date_key)
                                                                        FROM Dim_Date
                                                                        WHERE is_weekend = T2.is_weekend),
                                                                       0)                                     AS "Rata-rata Pinjaman Harian"
                                                          FROM Monthly_KPI_Summary AS T1
                                                                   JOIN Dim_Date AS T2
                                                                        ON T1.year = T2.year AND T1.month = T2.month
                                                          GROUP BY 1, T2.is_weekend
                                                          ORDER BY T2.is_weekend DESC;
                                                          """
}


def run_analytical_queries(db_path):
    if not os.path.exists(db_path):
        print(f"ERROR: Datamart not found in : {db_path}")
        print("Make sure you have done all of 14 task")
        return

    conn = None
    try:
        conn = sqlite3.connect(db_path)

        print(f"ANALYSIS DATA MART (DB: {os.path.basename(db_path)})")

        for title, query in QUERIES.items():
            print(f"\n{title}")

            df_result = pd.read_sql_query(query, conn)

            if df_result.empty:
                print("Query Not Found")
            else:
                print(df_result.to_markdown(index=False))

    except sqlite3.Error as e:
        print(f"\nError Sqlite: {e}")

    except Exception as e:
        print(f"\nError: {e}")

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    current_script_path = os.path.dirname(os.path.abspath(__file__))

    os.chdir(current_script_path)

    # run analytical query
    run_analytical_queries(DATAMART_DB_PATH)
