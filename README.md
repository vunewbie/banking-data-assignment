# Banking System Database

Simplified banking system với PostgreSQL 17 và Docker.

## 🚀 Quick Start

### 1. Build và Start Database:
```bash
docker-compose up -d --build
```

### 2. Connect to Database:
```bash
# Connection info:
Host: localhost
Port: 5432
Database: banking_system
Username: postgres  
Password: postgres
```

### 3. Test Connection:
```bash
docker exec -it banking-data-assignment-postgresdb-1 psql -U postgres -d banking_system -c "\dt"
```

## 📋 Database Schema

Database sẽ tự động tạo các bảng:
- **Customers** - Thông tin khách hàng
- **Face_Templates** - Mẫu sinh trắc học 
- **Bank_Accounts** - Tài khoản ngân hàng
- **Transactions** - Giao dịch (Transfer + Bill Payment)
- **Customer_Devices** - Thiết bị truy cập
- **Authentication_Logs** - Log xác thực

## 🛠 Commands

```bash
# Start database
docker-compose up -d

# Stop database  
docker-compose down

# View logs
docker-compose logs -f postgresdb

# Connect to database
docker exec -it banking-data-assignment-postgresdb-1 psql -U postgres -d banking_system

# Rebuild from scratch
docker-compose down -v && docker-compose up -d --build
```

## 📊 Next Steps

1. **Part 2**: Generate sample data với `generate_data.py`
2. **Part 3**: Data quality checks với `data_quality_standards.py` và `monitoring_audit.py`
