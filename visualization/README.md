# 🏦 Banking Data Quality Dashboard

## 📊 Overview

This Streamlit dashboard provides real-time visualization of banking data quality metrics and risk analysis. It displays:

- **🔴 Top Failed Data Quality Checks** - Monitor data quality audit results
- **⚠️ High-Risk Transaction Analysis** - Track risky financial transactions
- **📱 Unverified Devices by Customer** - Analyze device security status

## 🚀 Quick Start

### 1. Install Dependencies
```bash
cd visualization/
pip install -r requirements.txt
```

### 2. Ensure Database is Running
Make sure your PostgreSQL banking database is running on `localhost:5433`:
```bash
# From project root
docker-compose up -d banking-db
```

### 3. Launch Dashboard
```bash
cd visualization/
streamlit run dashboard.py
```

### 4. Access Dashboard
Open your browser and navigate to: `http://localhost:8501`

## 🎛️ Dashboard Features

### 📊 Interactive Controls
- **📅 Date Range Filter** - Select time period for analysis
- **🎯 Risk Level Filter** - Filter by High/Medium/Low risk
- **💰 Amount Range Slider** - Filter transactions by amount
- **🔄 Auto Refresh** - Enable real-time updates (30s interval)

### 📈 Visualizations

#### 🔴 Failed Checks Section
- Bar chart of top 10 failed checks
- Pie chart of failure distribution by severity
- Key metrics: total failed, average failure rate
- Detailed data table with all failed checks

#### ⚠️ Risky Transactions Section
- Time series chart of daily risky transaction volume
- Scatter plot showing amount vs risk score correlation
- Key metrics: total risky amount, transaction count
- Top 20 highest risk transactions table

#### 📱 Unverified Devices Section
- Device type distribution pie chart
- Top customers with most unverified devices
- Trust score histogram with risk level coloring
- Detailed device information table

## 🔧 Technical Details

### 🔌 Data Sources
- **Live Database**: PostgreSQL queries for transaction and device data
- **Audit Reports**: JSON files from `../reports/` directory
- **Log Files**: Analysis of audit logs from `../logs/scheduler/`

### 📊 Database Schema
The dashboard queries these main tables:
- `customer` - Customer information and risk ratings
- `transaction` - Financial transaction records
- `customer_device` - Device information and verification status
- `bank_account` - Account details linked to customers

### 🎨 UI Components
- **Layout**: Wide layout with sidebar controls
- **Charts**: Plotly interactive visualizations
- **Styling**: Custom CSS with banking color scheme
- **Responsiveness**: Mobile-friendly design

## 🛠️ Troubleshooting

### ❌ Common Issues

**Database Connection Failed**
```bash
# Check if database is running
docker ps | grep banking-db

# Restart if needed
docker-compose restart banking-db
```

**No Data Available**
```bash
# Run data generation first
cd ../
python src/generate_data.py

# Or trigger via Airflow DAG
```

**Port Already in Use**
```bash
# Change Streamlit port
streamlit run dashboard.py --server.port 8502
```

### 🔍 Debug Mode
Run with debug information:
```bash
streamlit run dashboard.py --logger.level=debug
```

## 📱 Mobile Access

The dashboard is responsive and works on mobile devices. Access via:
- **Desktop**: `http://localhost:8501`
- **Mobile**: `http://YOUR_IP:8501` (replace YOUR_IP with your machine's IP)

## 🔄 Auto Refresh

Enable auto-refresh in the sidebar to get real-time updates every 30 seconds. This is useful for monitoring live banking operations.

## 📋 Export Data

Use the expandable sections to view detailed tables. Data can be copied or exported directly from the Streamlit interface.

---

🏦 **Banking Data Quality Dashboard** | Powered by Streamlit | Real-time Banking Analytics 