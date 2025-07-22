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
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Database Setup

```bash
# Start PostgreSQL database with Docker
docker-compose up -d

# Verify database is running
docker-compose ps
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

### 4. Generate Sample Data

```bash
# Navigate to src directory
cd src

# Generate banking data (50 customers by default)
python generate_data.py

# Generate larger dataset (optional)
python generate_data.py --customers 100
```

### 5. Run Data Quality Audit

```bash
# Run comprehensive data quality audit
python monitoring_audit.py

# The audit will:
# - Validate data quality
# - Clean problematic records
# - Generate detailed reports in reports/
# - Save clean data to database
```

### 6. Launch Monitoring Dashboard

```bash
# Navigate to visualization directory
cd ../visualization

# Install dashboard dependencies (if not done globally)
pip install streamlit plotly

# Launch dashboard
streamlit run dashboard.py
```

Dashboard will be available at: `http://localhost:8501`

## 🔄 How to Run DAG or Job Scheduler

### Option 1: Airflow DAG (Recommended for Production)

#### Setup Airflow

```bash
# Install Airflow (Python 3.9+ required)
pip install apache-airflow

# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

#### Deploy DAG

```bash
# Copy DAG to Airflow DAGs folder
cp dags_or_jobs/banking_dq_dag.py ~/airflow/dags/

# Start Airflow webserver
airflow webserver --port 8080

# Start Airflow scheduler (in separate terminal)
airflow scheduler
```

#### Access Airflow UI

1. Navigate to `http://localhost:8080`
2. Login with admin credentials
3. Find and enable `banking_data_quality_dag`
4. The DAG will run daily at 2:00 AM automatically

#### Manual DAG Execution

```bash
# Trigger manual DAG run
airflow dags trigger banking_data_quality_dag

# Monitor DAG execution
airflow dags state banking_data_quality_dag <execution_date>
```

### Option 2: Standalone Script Execution

```bash
# Run complete pipeline manually
cd src
python monitoring_audit.py

# Or run individual components
python generate_data.py           # Generate data only
python data_quality_standards.py  # Quality checks only
```

### Option 3: Cron Job (Linux/macOS)

```bash
# Add to crontab for daily execution at 2:00 AM
crontab -e

# Add this line:
0 2 * * * cd /path/to/banking-data-assignment/src && python monitoring_audit.py
```

## 📋 Description of Assumptions Made

### 1. **Vietnamese Banking Context**

- **Regulatory Compliance:** Implements requirements from **2345/QĐ-NHNN 2023**
- **Currency:** All monetary amounts in **Vietnamese Dong (VND)**
- **Bank Code:** Uses **BVBank (Bank for Investment and Development of Vietnam)** format
- **Geographic Scope:** Covers **24 major provinces/cities** in Vietnam

### 2. **Customer Demographics**

- **Age Distribution:** Primary banking customers aged **18-70** years
- **KYC Completion Rate:** **85%** realistic completion rate
- **Document Types:** **CCCD (Citizen ID)** and **Passport** following Vietnamese law
- **Phone Numbers:** Valid Vietnamese mobile prefixes (090x, 091x, etc.)

### 3. **Business Logic Assumptions**

#### Financial Transactions:
- **High-value threshold:** Transactions ≥ **10M VND** require strong authentication
- **Daily limits:** Customers with > **20M VND/day** need additional verification
- **Authentication methods:** PIN, iOTP, Biometric based on transaction amount
- **Transaction types:** Internal Transfer, External Transfer, Bill Payment

#### Risk Assessment:
- **Occupation-based risk scoring:** Government jobs = low risk, freelance = high risk
- **Geographic risk:** Border provinces = higher risk
- **Device trust:** Based on usage history and verification status

### 4. **Data Quality Standards**

- **Uniqueness:** Customer IDs, phone numbers, account numbers must be globally unique
- **Format Validation:** Vietnamese phone (10-11 digits), CCCD (12 digits), Tax ID (10-13 digits)
- **Referential Integrity:** All foreign keys must reference valid parent records
- **Null Handling:** Critical fields (IDs, phone) cannot be null

### 5. **Technical Assumptions**

#### Database:
- **PostgreSQL 17** as primary database engine
- **UTF-8 encoding** for Vietnamese character support
- **Connection pooling** available for production loads

#### Security:
- **SHA-256 + salt** for password/PIN hashing
- **No raw credentials** stored in database
- **Encrypted face templates** using 128-dimension encoding

#### Performance:
- **Dataset size:** Optimized for **50-10,000 customers** for demonstration
- **Memory usage:** Designed for standard development machines (8GB+ RAM)
- **Processing time:** ~2-5 minutes for full pipeline with 1000 customers

### 6. **Operational Assumptions**

- **Daily execution:** Quality audits run once per day during off-peak hours
- **Error tolerance:** System continues operation if individual checks fail
- **Data retention:** Audit reports stored for compliance and forensic analysis
- **Monitoring:** Real-time dashboard for operational monitoring

### 7. **Compliance & Regulatory**

- **Data Privacy:** Customer data handled per Vietnamese data protection laws
- **Audit Trail:** Complete logging of all data quality issues and remediation
- **Regulatory Reporting:** Structured reports for banking compliance officers
- **Risk Management:** Integration with existing risk management frameworks

## 📁 Project Structure

```
banking-data-assignment/
├── sql/                    # Database schema and setup
│   └── schema.sql
├── src/                    # Core Python modules
│   ├── generate/          # Data generation modules
│   ├── generate_data.py   # Main data generator
│   ├── data_quality_standards.py  # Quality check engine
│   └── monitoring_audit.py        # Audit orchestrator
├── dags_or_jobs/          # Airflow DAG and scheduling
│   └── banking_dq_dag.py
├── visualization/         # Monitoring dashboard
│   └── dashboard.py
├── reports/               # Generated audit reports
├── docker-compose.yml     # Database containerization
└── requirements.txt       # Python dependencies
```

## 🔧 Troubleshooting

### Common Issues

1. **Database Connection Failed**
   ```bash
   # Restart Docker containers
   docker-compose down && docker-compose up -d
   ```

2. **Python Import Errors**
   ```bash
   # Ensure you're in the correct directory and venv is activated
   cd src
   python -c "import pandas; print('Dependencies OK')"
   ```

3. **Airflow DAG Not Appearing**
   ```bash
   # Check DAG syntax
   python dags_or_jobs/banking_dq_dag.py
   
   # Refresh Airflow
   airflow dags list
   ```

### Support

For technical support or questions about implementation details, please review the code documentation and inline comments throughout the project.

---

**Built for Timo Bank Data Engineer Internship Assessment**  
**Compliance:** Vietnamese Banking Regulation 2345/QĐ-NHNN 2023
