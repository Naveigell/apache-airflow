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
| Sqlite         | 2.6.0   |
| Faker          | 37.11.0 |

## Installation

| <span style="color: #c42333; font-size: 25px;">⚠️  DANGER</span>                   |
|------------------------------------------------------------------------------------|
| Make sure you are in the root folder and please use python 3 <br> for installation |

Run this command to install .venv
```bash
python -m venv .venv
```

Now activate the .venv using this command, make sure you are using linux or macos, if you are using windows, 
don't forget to install wsl or any linux terminal such like ```git bash```
```bash
source .venv/bin/activate
```

Run this command on your terminal or command prompt to install all the requirements:
```bash
pip install -r requirements.txt
```

Now copy the ```.env.example``` file into ```.env``` with this command
```bash
cp .env.example .env
```
and fill it with your own values, or keep the default values as it is

## Seeding
Run this command to seed the database:
```bash
python plugins/seeder.py
```

The ```.sqlite``` will shown raw folder that you have add it in ```.env``` file

## Server
Run this command to start the server:
```bash
airflow api-server -p 8080
```

and then open ```http://localhost:8080``` to access the airflow dashboard, and fill it with your username and password


Now run this command to make your dags available in the dashboard
```bash
airflow dag-processor
```

After you run ```dag-processor``` you need to run this command to start your scheduler
```bash
airflow scheduler
```

And now you can trigger your dags