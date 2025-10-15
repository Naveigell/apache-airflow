# Apache Airflow Installation and Running Guide

## Project Description
This project is a ETL pipeline that extracts data from a SQLite database, transforms it, and loads it into a staging database.
This project is a library management system ETL pipeline.

## Tech Stack

| Technology     | Version |
|----------------|---------|
| Python         | 3.12    |
| Apache Airflow | 3.0.6   |
| Pandas         | 2.3.3   |
| Sqlite         | latest  |
| Faker          | 37.11.0 |

## Installation

| ⚠️  WARNING                                                                        |
|------------------------------------------------------------------------------------|
| Make sure you are in the root folder and please use python 3 <br> for installation |

Run this code on your terminal or command prompt to install all the requirements:
```bash
pip install -r requirements.txt
```

## Seeding
Run this code to seed the database:
```bash
python plugins/seeder.py
```


