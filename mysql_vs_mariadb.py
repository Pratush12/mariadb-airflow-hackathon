import os
import csv
import json
import random
import string
import datetime
import time
import pandas as pd
import mariadb
import mysql.connector
import subprocess

# ---------------- CONFIG ----------------
CSV_FILE = "customers.csv"
N_ROWS = 500000  # configurable
DB_CONFIG = {
    "user": "myuser",
    "password": "MyStrongP@ssword!",
    "host": "127.0.0.1",
    "database": "test",
    "port": 3307

}

CITIES = ["New York", "London", "Berlin", "Tokyo", "Bangalore", "Paris"]
COUNTRIES = ["USA", "UK", "Germany", "Japan", "India", "France"]
FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]

CUSTOMER_COLUMNS = [
    "first_name", "last_name", "email", "phone",
    "city", "country", "signup_date", "last_login", "loyalty_points", "metadata"
]
# -----------------------------------------

def random_email(first, last):
    domains = ["gmail.com", "yahoo.com", "outlook.com", "example.com"]
    return f"{first.lower()}.{last.lower()}@{random.choice(domains)}"

def random_json():
    data = {
        "preferences": {
            "newsletter": random.choice([True, False]),
            "sms": random.choice([True, False])
        },
        "last_purchase": {
            "amount": round(random.uniform(10, 2000), 2),
            "item": random.choice(["Laptop", "Phone", "Shoes", "Book"])
        },
        "tags": random.sample(["vip", "electronics", "fashion", "new_customer", "loyal"], 2)
    }
    return json.dumps(data)

def generate_csv(file_path, n_rows):
    with open(file_path, "w", newline="") as f:
        writer = csv.writer(f)
        header = ["customer_id"] + CUSTOMER_COLUMNS
        writer.writerow(header)
        for i in range(1, n_rows + 1):
            first = random.choice(FIRST_NAMES)
            last = random.choice(LAST_NAMES)
            row = [
                i,
                first,
                last,
                random_email(first, last),
                f"+1-202-555-{random.randint(1000,9999)}",
                random.choice(CITIES),
                random.choice(COUNTRIES),
                (datetime.date.today() - datetime.timedelta(days=random.randint(0, 2000))).isoformat(),
                (datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365),
                                                             seconds=random.randint(0, 86400))).strftime("%Y-%m-%d %H:%M:%S"),
                random.randint(0, 5000),
                random_json()
            ]
            writer.writerow(row)
    print(f"Generated CSV {file_path} with {n_rows:,} customer records")

def setup_table(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id INT AUTO_INCREMENT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(100),
            phone VARCHAR(20),
            city VARCHAR(50),
            country VARCHAR(50),
            signup_date DATE,
            last_login DATETIME,
            loyalty_points INT,
            metadata JSON
        ) 
            """)
    cur.execute("TRUNCATE TABLE customers")
    conn.commit()
    cur.close()

# ---------- BENCHMARKS ----------
def benchmark_executemany(conn, data, batch_size=5000):
    """
    Insert data using executemany in batches to avoid connection loss.

    Args:
        conn: MariaDB/MySQL connection object
        data: List of tuples to insert
        batch_size: Number of rows per batch
    """
    setup_table(conn)
    cur = conn.cursor()
    placeholders = ", ".join(["%s"] * len(CUSTOMER_COLUMNS))
    sql = f"INSERT INTO customers ({', '.join(CUSTOMER_COLUMNS)}) VALUES ({placeholders})"

    start = time.time()
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        cur.executemany(sql, batch)
        conn.commit()
    elapsed = time.time() - start
    cur.close()
    return elapsed


def benchmark_select(conn):
    cur = conn.cursor()
    start = time.time()
    cur.execute("""SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS full_name,
    c.email,
    c.country,
    c.city,
    c.signup_date,
    c.loyalty_points,

    -- Extract JSON field safely
    JSON_UNQUOTE(JSON_EXTRACT(c.metadata, '$.preferences.language')) AS preferred_language,

    -- Compute days since signup
    DATEDIFF(CURDATE(), c.signup_date) AS days_since_signup,

    -- Rank by loyalty within each country
    RANK() OVER (PARTITION BY c.country ORDER BY c.loyalty_points DESC) AS rank_in_country,

    -- Average loyalty points per country (via window)
    AVG(c.loyalty_points) OVER (PARTITION BY c.country) AS avg_loyalty_country,

    -- Customer’s deviation from their country average
    c.loyalty_points - AVG(c.loyalty_points) OVER (PARTITION BY c.country) AS loyalty_gap,

    -- Category by loyalty
    CASE
        WHEN c.loyalty_points >= 1000 THEN 'Platinum'
        WHEN c.loyalty_points >= 500 THEN 'Gold'
        WHEN c.loyalty_points >= 100 THEN 'Silver'
        ELSE 'Bronze'
    END AS loyalty_tier

FROM customers c
WHERE c.signup_date IS NOT NULL
  AND c.loyalty_points IS NOT NULL

ORDER BY c.country, rank_in_country;
    """)
    # cur.fetchone()
    # cur.execute("SELECT * FROM customers LIMIT 1000")
    cur.fetchall()
    return time.time() - start

def benchmark_load_infile(conn, file_path):
    setup_table(conn)
    cur = conn.cursor()
    start = time.time()
    sql = f"""
        LOAD DATA LOCAL INFILE '{file_path}'
        INTO TABLE customers
        FIELDS TERMINATED BY ','
        IGNORE 1 LINES
        ({', '.join(CUSTOMER_COLUMNS)})
    """
    cur.execute(sql)
    conn.commit()
    return time.time() - start


def benchmark_json_insert(conn, data):
    return benchmark_executemany(conn, data)

def benchmark_json_select(conn):
    cur = conn.cursor()
    start = time.time()
    cur.execute("SELECT JSON_EXTRACT(metadata, '$.preferences.newsletter') FROM customers LIMIT 1000")
    cur.fetchall()
    return time.time() - start

def benchmark_json_update(conn):
    cur = conn.cursor()
    start = time.time()
    cur.execute("UPDATE customers SET metadata = JSON_SET(metadata, '$.preferences.sms', true) WHERE customer_id <= 1000")
    conn.commit()
    return time.time() - start

# ---------- MAIN ----------
def main():
    generate_csv(CSV_FILE, N_ROWS)

    # Prepare sample data for executemany (first 100k rows to save memory)
    sample_data = []
    with open(CSV_FILE, newline="") as f:
        reader = csv.reader(f)
        next(reader)
        for i, row in enumerate(reader):
            # skip customer_id in insert
            sample_data.append(tuple(row[1:]))
            # if i >= 100_000:
            #     break

    results = []

    # ---- MariaDB Connector ----
    mariadb_conn = mariadb.connect(**DB_CONFIG, local_infile=True)
    results.append(("executemany", benchmark_executemany(mariadb_conn, sample_data), None))
    results.append(("select", benchmark_select(mariadb_conn), None))
    results.append(("load_infile", benchmark_load_infile(mariadb_conn, CSV_FILE), None))
    results.append(("json_insert", benchmark_json_insert(mariadb_conn, sample_data), None))
    results.append(("json_select", benchmark_json_select(mariadb_conn), None))
    results.append(("json_update", benchmark_json_update(mariadb_conn), None))
    mariadb_conn.close()

    # ---- MySQL Connector ----
    mysql_conn = mysql.connector.connect(**DB_CONFIG, allow_local_infile=True)
    # fill mysql timings (skip cpimport)
    results = [
        (op,
         mariadb_time,
         benchmark_executemany(mysql_conn, sample_data) if op=="executemany" else
         benchmark_select(mysql_conn) if op=="select" else
         benchmark_load_infile(mysql_conn, CSV_FILE) if op=="load_infile" else
         None if op=="cpimport" else
         benchmark_json_insert(mysql_conn, sample_data) if op=="json_insert" else
         benchmark_json_select(mysql_conn) if op=="json_select" else
         benchmark_json_update(mysql_conn) if op=="json_update" else None
         )
        for (op, mariadb_time, _) in results
    ]
    mysql_conn.close()

    # ---- Save Results ----
    df = pd.DataFrame(results, columns=["operation", "mariadb_connector_sec", "mysql_connector_sec"])
    df.to_csv("benchmark_results.csv", index=False)
    print(df)

if __name__ == "__main__":
    main()
