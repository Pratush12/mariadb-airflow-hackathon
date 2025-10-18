# 🚀 Ctrl_Alt_db — Airflow MariaDB Connector

**Team Name:** Ctrl_Alt_db  
**Project Title:** Airflow MariaDB Connector  
**Theme:** Integration  

---

## 🧩 Problem Statement

Apache Airflow currently lacks **native integration with MariaDB**, forcing developers to rely on the MySQL connector.  
However, this connector is **incompatible with key MariaDB-specific features** such as:

- ⚡ **ColumnStore**
- 📥 **cpimport**
- 🧠 **Native JSON functions**

This limitation results in **reduced functionality** and **performance bottlenecks** in ETL workflows.  
Data pipelines built on Airflow cannot fully leverage **MariaDB’s high-performance architecture**.

> 🔍 **Benchmark Insight:**  
> The MariaDB Python connector outperforms the MySQL connector by up to **3×** in operations like:
> - `executemany`
> - `SELECT`
> - `JSON_INSERT`

The absence of a native Airflow–MariaDB connector thus limits Airflow’s ability to orchestrate **modern, high-performance, and scalable MariaDB data workflows**.

---

## 💡 Solution Overview

The **Airflow MariaDB Connector** introduces **seamless, native integration** between **Apache Airflow** and **MariaDB (including ColumnStore)**.

### ✅ Key Features

- 🧩 **Native Airflow connection type** for direct MariaDB integration (no MySQL fallback)
- 🚀 **High-speed data ingestion** using `cpimport`, optimized for bulk ETL operations
- 🔄 **ETL workflows**: download → transform → load between **MariaDB** and **S3**
- 📊 **Columnar architecture support** for faster analytical queries
- ⚙️ **3× performance improvement** over MySQL connector for critical database operations

---

## 🧠 Concept

### 🎯 Goal
Build a **seamless ETL integration** between **Apache Airflow** and **MariaDB ColumnStore**.

### 💭 Idea
Automate **OpenFlights** data ingestion using:
- **Airflow DAGs** for orchestration
- **Secure SSH transfers**
- **cpimport** for high-performance bulk loading into ColumnStore

---

## 🏗️ Principles & Design

| Principle   | Description |
|--------------|-------------|
| 🔁 **Automation** | Entire data pipeline runs automatically via Airflow scheduling |
| 🔐 **Security** | Uses SSH-based file transfer — no direct DB exposure |
| ⚖️ **Scalability** | ColumnStore ensures distributed & parallel data loading |
| 🧩 **Modularity** | Each dataset (airports, airlines, routes, etc.) is processed independently |
| 🔄 **Reusability** | DAG supports adding new datasets via simple JSON config updates |

---

## ⚙️ Installation & Setup Instructions

Follow these steps to set up and run the **Airflow MariaDB Connector project** locally.

---

### 🧱 Step 1: Add Custom Provider to Airflow

Since Airflow doesn’t natively support MariaDB, we created a **custom provider**.

This provider:
- Adds **MariaDB connection type**
- Provides **S3 hooks** and **cpimport operators**
- Enables **direct integration** with MariaDB from Airflow DAGs

```bash
# Clone or copy your provider into the airflow directory
COPY ./airflow-mariadb-provider /opt/airflow/.local/src/airflow-mariadb-provider

# Install the provider
pip install -e /opt/airflow/.local/src/airflow-mariadb-provider
```

### 🐬 Step 2: Install MariaDB with ColumnStore Engine

We use MariaDB ColumnStore inside Docker for high-performance analytical queries.

```bash
docker run -d -p 3307:3306 -p 2222:22 --shm-size=512m -e PM1=mcs1 --hostname=mcs1 mariadb/columnstore
docker exec -it mcs1 provision mcs1
```

🧠 Why ColumnStore?
It enables parallelized columnar data storage — perfect for analytical workloads.

### ⚓ Step 3: Connect Airflow to MariaDB via Docker Network

```bash
docker network connect airflow_net mcs1
docker-compose down -v
docker-compose up -d
```

Your docker-compose.yml connects both containers (Airflow + MariaDB) via the same network for smooth communication.

### 🐳 Step 4: Dockerfile for Airflow with MariaDB Connector

```bash
# Use the official Airflow image
FROM apache/airflow:2.9.0

USER root

# Install dependencies for MariaDB
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc \
        libmariadb-dev \
        mariadb-client && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
ENV PATH="/home/airflow/.local/bin:${PATH}"

# Install Python MariaDB driver
RUN pip install --no-cache-dir mariadb

# Copy and install custom provider
COPY --chown=airflow:airflow ./airflow-mariadb-provider /opt/airflow/.local/src/airflow-mariadb-provider
RUN pip install --no-cache-dir -e /opt/airflow/.local/src/airflow-mariadb-provider
```

### 🔐 Step 5: Enable SSH Connection in MariaDB Container

SSH is used for secure file transfers (e.g., CSV → cpimport).

```bash
docker exec -it mcs1 bash
ssh-keygen -A
/usr/sbin/sshd -D &
exit
```

Then restart the Airflow webserver:

```bash
docker restart airflow-docker-airflow-webserver-1
```