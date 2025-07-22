# Banking Data Quality Management System

## Project Overview

This project implements a simplified **Banking Data Quality Management System** designed for regulatory compliance with **Vietnamese banking regulations (2345/QĐ-NHNN 2023)**. The system provides end-to-end data generation, quality auditing, data cleaning and monitoring capabilities for a fictional banking environment.

### Key Features

- **Realistic Vietnamese Banking Data Generation** with business logic compliance
- **Comprehensive Data Quality Checks** including uniqueness, format validation, and business rules
- **Risk-based Transaction Authentication** validation per regulatory requirements
- **Automated Scheduling & Orchestration** with Airflow DAG
- **Real-time Monitoring Dashboard** with interactive visualizations
- **Database Integration** with PostgreSQL and Docker containerization

### System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
|   ┌──────────────────────────────────────────────────────────────────────────────────────────┐  |
|   │                                Airflow Scheduler (DAG)                                   │  |
|   │                             Orchestrates Entire Pipeline                                 │  |
|   └──────────────────────────────────────────────────────────────────────────────────────────┘  |
|                                               │                                                 |
|                                               ▼                                                 |
|   ┌─────────────────┐    ┌─────────────────────┐    ┌─────────────────┐    ┌─────────────────┐  |
|   │ Data Generation │ => | Data Quality Check, │ => │  Data Loading   │ => │   Dashboard     │  |
|   │    (Python)     │    │   Clean And Audit   │    │  (PostgreSQL)   │    │  (Streamlit)    │  |
|   │                 │    │    (Python)         │    │                 │    │                 │  |
|   └─────────────────┘    └─────────────────────┘    └─────────────────┘    └─────────────────┘  |
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Database Schema

The system implements 6 core banking tables:
- **`customer`** - Customer information
- **`face_template`** - Biometric face templates with encrypted encoding
- **`bank_account`** - Account management with balance and limits
- **`customer_device`** - Device trust management and verification
- **`transaction`** - Financial transactions with fraud detection
- **`authentication_log`** - Authentication events and security logs

## Setup Instructions

### Prerequisites

- **Python 3.12**
- **Docker & Docker Compose** (for PostgreSQL database and Aiflow Scheduler)
- **Git** for version control

### 1. Clone Repository

```bash
git clone <repository-url>
cd banking-data-assignment
```

### 2. Python Environment Setup

```bash
pip install -r requirements.txt
```

### 3. Database Setup

```bash
# Start PostgreSQL database with Docker
docker compose up -d --build

# Verify database is running
docker compose ps
```

The database will be available at:
- **Host:** `localhost`
- **Port:** `5433`
- **Database:** `banking_system`
- **Username:** `postgres`
- **Password:** `postgres`

The airflow will be available at:
- **Host:** `localhost`
- **Port:** `5434`
- **Database:** `airflow`
- **Username:** `airflow`
- **Password:** `airflow`

### 4. Airflow DAG

- Navigate to `http://localhost:8080`
- Login with `username` = `airflow` and password = `airflow`
- Find and enable `banking_data_quality_dag`
- The DAG will start.

### 5. Launch Monitoring Dashboard

```bash
streamlit run ./visualization/dashboard.py
```

Dashboard will be available at: `http://localhost:8501`

## Project Structure

```
banking-data-assignment/
├── dags_or_jobs/          # Airflow DAG and scheduling
│   └── banking_dq_dag.py
├── docs/                  # Documents
├── logs/                  # Generated audit logs
├── reports/               # Generated audit reports
├── sql/                    # Database schema and setup
│   └── schema.sql
├── src/                    # Core Python modules
│   ├── generate/          # Data generation modules
│   ├── generate_data.py   # Main data generator
│   ├── data_quality_standards.py  # Quality check engine
│   └── monitoring_audit.py        # Audit orchestrator
├── visualization/         # Monitoring dashboard
│   └── dashboard.py
├── docker-compose.yml     
├── Dockerfile.db     
├── REAME.md     
└── requirements.txt
```
