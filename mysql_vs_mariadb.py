"""
MySQL vs MariaDB Performance Benchmark Script

This script performs comprehensive performance comparisons between MySQL and MariaDB
connectors across various database operations including bulk inserts, complex queries,
and JSON operations.

"""

import os
import csv
import json
import random
import datetime
import time
import logging
from typing import List, Tuple, Dict, Any, Optional
from dataclasses import dataclass
from contextlib import contextmanager

import pandas as pd
import mariadb
import mysql.connector

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    user: str
    password: str
    host: str
    database: str
    port: int

# Database connection settings
DB_CONFIG = DatabaseConfig(
    user="myuser",
    password="MyStrongP@ssword!",
    host="127.0.0.1",
    database="test",
    port=3307
)

# Benchmark configuration
CSV_FILE = "customers.csv"
N_ROWS =   500000  # Number of rows to generate for benchmarking
BATCH_SIZE = 5000  # Batch size for bulk operations

# Sample data for generating realistic test data
CITIES = ["New York", "London", "Berlin", "Tokyo", "Bangalore", "Paris"]
COUNTRIES = ["USA", "UK", "Germany", "Japan", "India", "France"]
FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]

# Database table schema
CUSTOMER_COLUMNS = [
    "first_name", "last_name", "email", "phone",
    "city", "country", "signup_date", "last_login", "loyalty_points", "metadata"
]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def generate_random_email(first_name: str, last_name: str) -> str:
    """
    Generate a realistic email address from first and last names.
    
    Args:
        first_name: Customer's first name
        last_name: Customer's last name
        
    Returns:
        Formatted email address
    """
    domains = ["gmail.com", "yahoo.com", "outlook.com", "example.com"]
    return f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"


def generate_random_json_metadata() -> str:
    """
    Generate realistic JSON metadata for customer records.
    
    Returns:
        JSON string containing customer preferences and purchase history
    """
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


@contextmanager
def database_connection(connector_type: str, config: DatabaseConfig):
    """
    Context manager for database connections with proper cleanup.
    
    Args:
        connector_type: Either 'mariadb' or 'mysql'
        config: Database configuration
        
    Yields:
        Database connection object
    """
    conn = None
    try:
        if connector_type == 'mariadb':
            conn = mariadb.connect(
                user=config.user,
                password=config.password,
                host=config.host,
                database=config.database,
                port=config.port,
                local_infile=True
            )
        elif connector_type == 'mysql':
            conn = mysql.connector.connect(
                user=config.user,
                password=config.password,
                host=config.host,
                database=config.database,
                port=config.port,
                allow_local_infile=True
            )
        else:
            raise ValueError(f"Unsupported connector type: {connector_type}")
        
        logger.info(f"Connected to {connector_type.upper()} database: {config.database}")
        yield conn
        
    except Exception as e:
        logger.error(f"Database connection error ({connector_type}): {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info(f"Closed {connector_type.upper()} connection")

# =============================================================================
# DATA GENERATION FUNCTIONS
# =============================================================================

def generate_test_csv(file_path: str, n_rows: int) -> None:
    """
    Generate a CSV file with realistic customer test data.
    
    Args:
        file_path: Path where the CSV file will be created
        n_rows: Number of customer records to generate
    """
    logger.info(f"Generating {n_rows:,} customer records...")
    
    with open(file_path, "w", newline="", encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write header row
        header = ["customer_id"] + CUSTOMER_COLUMNS
        writer.writerow(header)
        
        # Generate customer records
        for i in range(1, n_rows + 1):
            first_name = random.choice(FIRST_NAMES)
            last_name = random.choice(LAST_NAMES)
            
            # Generate realistic dates
            signup_date = (datetime.date.today() - 
                          datetime.timedelta(days=random.randint(0, 2000))).isoformat()
            
            last_login = (datetime.datetime.now() - 
                         datetime.timedelta(
                             days=random.randint(0, 365),
                             seconds=random.randint(0, 86400)
                         )).strftime("%Y-%m-%d %H:%M:%S")
            
            # Create customer record
            customer_record = [
                i,  # customer_id
                first_name,
                last_name,
                generate_random_email(first_name, last_name),
                f"+1-202-555-{random.randint(1000, 9999)}",  # phone
                random.choice(CITIES),
                random.choice(COUNTRIES),
                signup_date,
                last_login,
                random.randint(0, 5000),  # loyalty_points
                generate_random_json_metadata()
            ]
            
            writer.writerow(customer_record)
            
            # Progress logging for large datasets
            if i % 50000 == 0:
                logger.info(f"Generated {i:,} records...")
    
    logger.info(f"✅ Generated CSV {file_path} with {n_rows:,} customer records")

# =============================================================================
# DATABASE SETUP FUNCTIONS
# =============================================================================

def setup_customers_table(conn, table_name: str = "customers") -> None:
    """
    Create and prepare the customers table for benchmarking.
    
    Args:
        conn: Database connection object
        table_name: Name of the table to create (e.g., 'customers_mariadb', 'customers_mysql')
    """
    logger.info(f"Setting up {table_name} table...")
    
    with conn.cursor() as cur:
        # Create table if it doesn't exist
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
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
        """
        cur.execute(create_table_sql)
        
        # Clear existing data for clean benchmark
        cur.execute(f"TRUNCATE TABLE {table_name}")
    conn.commit()
        
    logger.info(f"✅ {table_name} table setup completed")

# =============================================================================
# BENCHMARK FUNCTIONS
# =============================================================================

def benchmark_executemany(conn, data: List[Tuple], table_name: str = "customers", batch_size: int = BATCH_SIZE) -> float:
    """
    Benchmark bulk insert operations using executemany.

    Args:
        conn: Database connection object
        data: List of tuples containing customer data to insert
        table_name: Name of the table to insert into
        batch_size: Number of rows to insert per batch
        
    Returns:
        Execution time in seconds
    """
    logger.info(f"Benchmarking executemany with {len(data):,} rows (batch size: {batch_size:,}) into {table_name}")
    
    # Setup table for clean benchmark
    setup_customers_table(conn, table_name)
    
    # Prepare SQL statement
    placeholders = ", ".join(["%s"] * len(CUSTOMER_COLUMNS))
    insert_sql = f"INSERT INTO {table_name} ({', '.join(CUSTOMER_COLUMNS)}) VALUES ({placeholders})"
    
    start_time = time.time()
    
    with conn.cursor() as cur:
        # Insert data in batches
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            cur.executemany(insert_sql, batch)
            conn.commit()
            
            # Progress logging for large datasets
            if (i + batch_size) % 50000 == 0:
                logger.info(f"Inserted {min(i + batch_size, len(data)):,} rows into {table_name}...")
    
    elapsed_time = time.time() - start_time
    logger.info(f"✅ Executemany completed in {elapsed_time:.2f} seconds for {table_name}")
    return elapsed_time


def benchmark_complex_select(conn, table_name: str = "customers") -> float:
    """
    Benchmark complex SELECT queries with window functions and JSON operations.
    
    Args:
        conn: Database connection object
        table_name: Name of the table to query
        
    Returns:
        Execution time in seconds
    """
    logger.info(f"Benchmarking complex SELECT query with window functions and JSON on {table_name}...")
    
    # Complex query with window functions, JSON operations, and aggregations
    complex_select_sql = f"""
        SELECT
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

            -- Customer's deviation from their country average
    c.loyalty_points - AVG(c.loyalty_points) OVER (PARTITION BY c.country) AS loyalty_gap,

    -- Category by loyalty
    CASE
        WHEN c.loyalty_points >= 1000 THEN 'Platinum'
        WHEN c.loyalty_points >= 500 THEN 'Gold'
        WHEN c.loyalty_points >= 100 THEN 'Silver'
        ELSE 'Bronze'
    END AS loyalty_tier

        FROM {table_name} c
WHERE c.signup_date IS NOT NULL
  AND c.loyalty_points IS NOT NULL
        ORDER BY c.country, rank_in_country
    """
    
    start_time = time.time()
    
    with conn.cursor() as cur:
        cur.execute(complex_select_sql)
        results = cur.fetchall()
    
    elapsed_time = time.time() - start_time
    logger.info(f"✅ Complex SELECT completed in {elapsed_time:.2f} seconds ({len(results):,} rows) from {table_name}")
    return elapsed_time

def benchmark_load_data_infile(conn, file_path: str, table_name: str = "customers") -> float:
    """
    Benchmark LOAD DATA INFILE operations for bulk data loading.
    
    Args:
        conn: Database connection object
        file_path: Path to the CSV file to load
        table_name: Name of the table to load data into
        
    Returns:
        Execution time in seconds
    """
    logger.info(f"Benchmarking LOAD DATA INFILE from {file_path} into {table_name}")
    
    # Setup table for clean benchmark
    setup_customers_table(conn, table_name)
    
    load_data_sql = f"""
        LOAD DATA LOCAL INFILE '{file_path}'
        INTO TABLE {table_name}
        FIELDS TERMINATED BY ','
        IGNORE 1 LINES
        ({', '.join(CUSTOMER_COLUMNS)})
    """
    
    start_time = time.time()
    
    with conn.cursor() as cur:
        cur.execute(load_data_sql)
    conn.commit()
    
    elapsed_time = time.time() - start_time
    logger.info(f"✅ LOAD DATA INFILE completed in {elapsed_time:.2f} seconds for {table_name}")
    return elapsed_time


def benchmark_json_insert(conn, data: List[Tuple], table_name: str = "customers") -> float:
    """
    Benchmark JSON insert operations (same as executemany but focused on JSON data).
    
    Args:
        conn: Database connection object
        data: List of tuples containing customer data with JSON metadata
        table_name: Name of the table to insert into
        
    Returns:
        Execution time in seconds
    """
    logger.info(f"Benchmarking JSON insert operations into {table_name}...")
    return benchmark_executemany(conn, data, table_name)


def benchmark_json_select(conn, table_name: str = "customers") -> float:
    """
    Benchmark JSON field extraction operations.
    
    Args:
        conn: Database connection object
        table_name: Name of the table to query
        
    Returns:
        Execution time in seconds
    """
    logger.info(f"Benchmarking JSON SELECT operations on {table_name}...")
    
    json_select_sql = f"SELECT JSON_EXTRACT(metadata, '$.preferences.newsletter') FROM {table_name} LIMIT 1000"
    
    start_time = time.time()
    
    with conn.cursor() as cur:
        cur.execute(json_select_sql)
        results = cur.fetchall()
    
    elapsed_time = time.time() - start_time
    logger.info(f"✅ JSON SELECT completed in {elapsed_time:.2f} seconds ({len(results):,} rows) from {table_name}")
    return elapsed_time


def benchmark_json_update(conn, table_name: str = "customers") -> float:
    """
    Benchmark JSON field update operations.
    
    Args:
        conn: Database connection object
        table_name: Name of the table to update
        
    Returns:
        Execution time in seconds
    """
    logger.info(f"Benchmarking JSON UPDATE operations on {table_name}...")
    
    json_update_sql = f"""
        UPDATE {table_name} 
        SET metadata = JSON_SET(metadata, '$.preferences.sms', true) 
        WHERE customer_id <= 1000
    """
    
    start_time = time.time()
    
    with conn.cursor() as cur:
        cur.execute(json_update_sql)
    conn.commit()
    
    elapsed_time = time.time() - start_time
    logger.info(f"✅ JSON UPDATE completed in {elapsed_time:.2f} seconds for {table_name}")
    return elapsed_time

# =============================================================================
# BENCHMARK EXECUTION AND RESULTS
# =============================================================================

def load_sample_data(file_path: str) -> List[Tuple]:
    """
    Load sample data from CSV file for benchmarking.
    
    Args:
        file_path: Path to the CSV file

    Returns:
        List of tuples containing customer data
    """
    logger.info(f"Loading sample data from {file_path}")
    
    sample_data = []
    with open(file_path, newline="", encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        
        for i, row in enumerate(reader):
            # Skip customer_id in insert (first column)
            sample_data.append(tuple(row[1:]))
            

    logger.info(f"✅ Loaded {len(sample_data):,} sample records")
    return sample_data


def run_benchmark_suite(connector_type: str, sample_data: List[Tuple], csv_file: str) -> Dict[str, float]:
    """
    Run the complete benchmark suite for a specific database connector.
    
    Args:
        connector_type: Either 'mariadb' or 'mysql'
        sample_data: Sample data for insert benchmarks
        csv_file: Path to CSV file for LOAD DATA INFILE benchmark
        
    Returns:
        Dictionary with benchmark results
    """
    logger.info(f"🚀 Starting benchmark suite for {connector_type.upper()}")
    
    # Create table name based on connector type
    table_name = f"customers_{connector_type}"
    
    results = {}
    
    with database_connection(connector_type, DB_CONFIG) as conn:
        # Benchmark executemany operations
        results['executemany'] = benchmark_executemany(conn, sample_data, table_name)
        
        # Benchmark complex SELECT operations
        results['select'] = benchmark_complex_select(conn, table_name)
        
        # Benchmark LOAD DATA INFILE operations
        results['load_infile'] = benchmark_load_data_infile(conn, csv_file, table_name)
        
        # Benchmark JSON insert operations
        results['json_insert'] = benchmark_json_insert(conn, sample_data, table_name)
        
        # Benchmark JSON SELECT operations
        results['json_select'] = benchmark_json_select(conn, table_name)
        
        # Benchmark JSON UPDATE operations
        results['json_update'] = benchmark_json_update(conn, table_name)
    
    logger.info(f"✅ Completed benchmark suite for {connector_type.upper()} using table {table_name}")
    return results


def generate_benchmark_report(mariadb_results: Dict[str, float], mysql_results: Dict[str, float]) -> None:
    """
    Generate and display comprehensive benchmark report.
    
    Args:
        mariadb_results: MariaDB benchmark results
        mysql_results: MySQL benchmark results
    """
    logger.info("📊 Generating benchmark report...")
    
    # Create results DataFrame
    operations = list(mariadb_results.keys())
    results_data = []
    
    for operation in operations:
        mariadb_time = mariadb_results.get(operation, 0)
        mysql_time = mysql_results.get(operation, 0)
        speedup_ratio = mysql_time / mariadb_time if mariadb_time > 0 else 0
        
        results_data.append({
            'operation': operation,
            'mariadb_connector_sec': mariadb_time,
            'mysql_connector_sec': mysql_time,
            'speedup_ratio': speedup_ratio
        })
    
    # Create DataFrame and save results
    df = pd.DataFrame(results_data)
    df.to_csv("benchmark_results.csv", index=False)
    
    # Display results
    print("\n" + "="*80)
    print("🏆 MYSQL vs MARIADB BENCHMARK RESULTS")
    print("="*80)
    print(df.to_string(index=False, float_format='%.2f'))
    print("="*80)
    
    # Calculate and display summary statistics
    speedup_ratios = df['speedup_ratio'].dropna()
    if not speedup_ratios.empty:
        avg_speedup = speedup_ratios.mean()
        mariadb_wins = sum(1 for ratio in speedup_ratios if ratio > 1)
        mysql_wins = sum(1 for ratio in speedup_ratios if ratio < 1)
        ties = sum(1 for ratio in speedup_ratios if ratio == 1)
        
        print(f"\n📈 PERFORMANCE SUMMARY:")
        print(f"   Average MariaDB speedup: {avg_speedup:.2f}x")
        print(f"   MariaDB wins: {mariadb_wins}")
        print(f"   MySQL wins: {mysql_wins}")
        print(f"   Ties: {ties}")
        
        if avg_speedup > 1:
            print(f"\n🏆 MariaDB connector is {avg_speedup:.2f}x faster overall!")
        elif avg_speedup < 1:
            print(f"\n🏆 MySQL connector is {1/avg_speedup:.2f}x faster overall!")
        else:
            print(f"\n🤝 Both connectors perform similarly!")
    
    print(f"\n💾 Detailed results saved to: benchmark_results.csv")
    print("="*80)


def main():
    """
    Main function to execute the complete MySQL vs MariaDB benchmark.
    """
    logger.info("🎯 Starting MySQL vs MariaDB Performance Benchmark")
    logger.info(f"Configuration: {N_ROWS:,} rows, batch size: {BATCH_SIZE:,}")
    
    try:
        # Step 1: Generate test data
        logger.info("📝 Step 1: Generating test data...")
        generate_test_csv(CSV_FILE, N_ROWS)
        
        # Step 2: Load sample data for benchmarking
        logger.info("📊 Step 2: Loading sample data...")
        sample_data = load_sample_data(CSV_FILE)
        
        # Step 3: Run MariaDB benchmarks
        logger.info("🔵 Step 3: Running MariaDB benchmarks...")
        mariadb_results = run_benchmark_suite('mariadb', sample_data, CSV_FILE)
        
        # Step 4: Run MySQL benchmarks
        logger.info("🟠 Step 4: Running MySQL benchmarks...")
        mysql_results = run_benchmark_suite('mysql', sample_data, CSV_FILE)
        
        # Step 5: Generate and display results
        logger.info("📈 Step 5: Generating benchmark report...")
        generate_benchmark_report(mariadb_results, mysql_results)
        
        logger.info("✅ Benchmark completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Benchmark failed: {e}")
        raise
    
    finally:
        # Cleanup
        if os.path.exists(CSV_FILE):
            os.remove(CSV_FILE)
            logger.info(f"🧹 Cleaned up temporary file: {CSV_FILE}")


if __name__ == "__main__":
    main()
