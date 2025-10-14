from __future__ import annotations

import os
import pathlib
from datetime import timedelta, datetime

from airflow.models.dag import DAG

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


# Impor fungsi-fungsi dari direktori scripts
# Airflow harus dikonfigurasi agar dapat menemukan modul-modul ini,
# atau bisa menggunakan relative path/sys.path manipulation,
# tapi untuk kesederhanaan, kita asumsikan dapat diimpor.

# Catatan: Dalam lingkungan Airflow yang sebenarnya, Anda mungkin perlu
# membuat package Python yang benar atau menggunakan mekanisme lain agar impor ini berhasil.
# Untuk contoh ini, kita asumsikan file scripts berada di Python Path atau
# kita akan menggunakan cara yang lebih sederhana seperti memanggil script via Bash.
# Di sini, saya akan *mengasumsikan* impor berhasil untuk menggunakan PythonOperator.

# Jika menggunakan PythonOperator, Anda harus memastikan fungsi-fungsi tersebut dapat diakses.
# Jika tidak, menggunakan BashOperator untuk memanggil 'python scripts/extract.py' adalah alternatif yang lebih kuat.

# --- Menggunakan PythonOperator dengan fungsi placeholder/dummy ---
# Untuk menghindari masalah impor, saya akan definisikan fungsi dummy di sini
# atau Anda harus memastikan 'scripts' dapat diimpor.
# Jika Anda benar-benar menggunakannya, ganti baris ini dengan impor yang sebenarnya:
# from scripts.extract import extract_data_from_source
# from scripts.transform import transform_raw_data
# from scripts.load import load_staging_to_datamart

def extract_data_task():
    with open(os.path.dirname(os.path.abspath(__file__)) + "/demofile.txt", "a") as f:
        f.write("Now the file has more content!")
    print("Simulasi menjalankan scripts/extract.py")
    return "data/raw/output.csv"  # Mengembalikan path data mentah


def transform_data_task(ti):
    raw_path = ti.xcom_pull(task_ids='extract_data')
    print(f"Simulasi menjalankan scripts/transform.py pada {raw_path}")
    return "data/staging/transformed.csv"  # Mengembalikan path data staging


def load_data_task(ti):
    staging_path = ti.xcom_pull(task_ids='transform_data')
    print(f"Simulasi menjalankan scripts/load.py untuk memuat {staging_path} ke datamart")

def notify_failure():
    pass

# --- Konfigurasi DAG ---
with DAG(
        dag_id="etl_to_datamart",
        description="A simple tutorial DAG",
        schedule=timedelta(seconds=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=["example"],
        default_args={
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(seconds=5),
            # 'queue': 'bash_queue',
            # 'pool': 'backfill',
            # 'priority_weight': 10,
            # 'end_date': datetime(2016, 1, 1),
            # 'wait_for_downstream': False,
            # 'execution_timeout': timedelta(seconds=300),
            # 'on_failure_callback': some_function, # or list of functions
            # 'on_success_callback': some_other_function, # or list of functions
            # 'on_retry_callback': another_function, # or list of functions
            # 'sla_miss_callback': yet_another_function, # or list of functions
            # 'on_skipped_callback': another_function, #or list of functions
            # 'trigger_rule': 'all_success'
        },
) as dag:
    # 1. Persiapan: Menjalankan SQL untuk membuat tabel (dari sql/create_tables.sql)
    create_tables = BashOperator(
        task_id="create_tables_if_not_exists",
        bash_command='echo "Menjalankan script SQL: cat sql/create_tables.sql | psql"',
        # Dalam implementasi nyata, Anda akan menggunakan Hook Airflow untuk terhubung ke DB
    )

    # 2. Ekstraksi Data (dari scripts/extract.py)
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_task,
        # op_kwargs={'source_path': 'config/config.yaml', 'raw_path': 'data/raw/'},
    )

    # 3. Transformasi Data (dari scripts/transform.py)
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data_task,
        # op_kwargs={'staging_path': 'data/staging/'},
    )

    # 4. Pemuatan Data (dari scripts/load.py)
    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_data_task,
        # op_kwargs={'datamart_path': 'data/datamart/'},
    )

    # 5. Opsional: Query Verifikasi (dari sql/queries.sql)
    run_verification_query = BashOperator(
        task_id="run_verification_query",
        bash_command='echo "Menjalankan query verifikasi: cat sql/queries.sql | psql"',
    )

    # 6. Pembersihan Data/Script
    cleanup_raw_files = BashOperator(
        task_id="cleanup_raw_files",
        bash_command='echo "Membersihkan file di data/raw/"',
        trigger_rule='all_success',  # Pastikan ini berjalan hanya jika semuanya berhasil
    )

    # --- Definisi Alur Kerja (Dependencies) ---

    # create_tables harus selesai sebelum extract_data dimulai
    create_tables >> extract_data

    # extract_data harus selesai sebelum transform_data dimulai
    extract_data >> transform_data

    # transform_data harus selesai sebelum load_data dimulai
    transform_data >> load_data

    # load_data harus selesai sebelum run_verification_query dan cleanup_raw_files dimulai
    load_data >> [run_verification_query, cleanup_raw_files]