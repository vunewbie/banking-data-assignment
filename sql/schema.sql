-- =====================================================
-- BANKING SYSTEM DATABASE SCHEMA
-- =====================================================

-- Extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =====================================================
-- customer TABLE
-- =====================================================
CREATE TABLE customer (
    customer_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    -- Basic information
    full_name VARCHAR(255) NOT NULL,
    gender VARCHAR(10) CHECK (gender IN ('Male', 'Female')),
    date_of_birth DATE NOT NULL,
    phone_number VARCHAR(20) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE,

    -- Identity information
    tax_identification_number VARCHAR(50) UNIQUE,
    id_passport_number VARCHAR(50) UNIQUE NOT NULL,
    issue_date DATE,
    expiry_date DATE,
    issuing_authority VARCHAR(255),

    -- Residence and work information
    is_resident BOOLEAN DEFAULT TRUE,
    occupation VARCHAR(255),
    position VARCHAR(255),
    work_address TEXT,
    residential_address TEXT NOT NULL,
    contact_address TEXT,

    -- Authentication information
    pin VARCHAR(255) NOT NULL, -- Encrypted/hashed PIN
    last_login_at TIMESTAMPTZ,
    failed_login_attempts INT DEFAULT 0,
    account_locked_until TIMESTAMPTZ,
    password VARCHAR(255) NOT NULL,
    password_last_changed TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    kyc_completed_at TIMESTAMPTZ,

    -- Business information
    risk_rating VARCHAR(20) CHECK (risk_rating IN ('Low', 'Medium', 'High')),
    risk_score DECIMAL(5,2) DEFAULT 00.00 CHECK (risk_score BETWEEN 0.00 AND 100.00),
    customer_type VARCHAR(50) DEFAULT 'Individual' CHECK (customer_type IN ('Individual', 'Organization')),
    monthly_income DECIMAL(15,2),

    -- System and Audit information
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'Active' CHECK (status IN ('Active', 'Inactive', 'Suspended', 'Closed')),
    
    -- Notification information
    sms_notification_enabled BOOLEAN DEFAULT TRUE,
    email_notification_enabled BOOLEAN DEFAULT TRUE
);

-- customer INDEXES
-- Partial index for business-critical KYC compliance queries
CREATE INDEX idx_customer_kyc_incomplete ON customer(customer_id) WHERE kyc_completed_at IS NULL;

-- =====================================================
-- face_template TABLE
-- =====================================================
CREATE TABLE face_template (
    template_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id UUID NOT NULL UNIQUE,
    encrypted_face_encoding BYTEA NOT NULL,
    last_used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- face_template CONSTRAINTS
ALTER TABLE face_template ADD CONSTRAINT fk_face_template_customer 
FOREIGN KEY (customer_id) REFERENCES customer(customer_id) 
ON DELETE CASCADE ON UPDATE CASCADE;

-- =====================================================
-- bank_account TABLE
-- =====================================================
CREATE TABLE bank_account (
    -- Identity & Ownership
    account_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id UUID NOT NULL,
    account_number VARCHAR(20) UNIQUE NOT NULL,

    -- Account type and primary status
    account_type VARCHAR(50) DEFAULT 'Savings' CHECK (account_type IN ('Savings', 'Current', 'Fixed_Deposit', 'Loan')),
    is_primary BOOLEAN DEFAULT FALSE,
    
    -- Balance Management
    available_balance DECIMAL(15,2) DEFAULT 0.00 CHECK (available_balance >= 0),
    current_balance DECIMAL(15,2) DEFAULT 0.00 CHECK (current_balance >= 0),
    hold_amount DECIMAL(15,2) DEFAULT 0.00 CHECK (hold_amount >= 0),
    currency VARCHAR(3) DEFAULT 'VND' CHECK (currency IN ('VND', 'USD', 'EUR')),

    -- Daily limits
    daily_transfer_limit DECIMAL(15,2) DEFAULT 50000000.00 CHECK (daily_transfer_limit > 0),
    daily_online_payment_limit DECIMAL(15,2) DEFAULT 20000000.00 CHECK (daily_online_payment_limit > 0),

    -- Account Controls
    status VARCHAR(20) DEFAULT 'Active' CHECK (status IN ('Active', 'Inactive', 'Suspended', 'Closed')),
    is_online_payment_enabled BOOLEAN DEFAULT TRUE,

    -- Interest
    interest_rate DECIMAL(5,4) DEFAULT 0.0000,

    -- Timestamps
    open_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_transaction_at TIMESTAMPTZ
);

-- bank_account CONSTRAINTS
ALTER TABLE bank_account ADD CONSTRAINT fk_accounts_customer 
FOREIGN KEY (customer_id) REFERENCES customer(customer_id) 
ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE bank_account ADD CONSTRAINT chk_balance_consistency 
CHECK (current_balance = available_balance + hold_amount);

-- bank_account INDEXES
CREATE INDEX idx_accounts_customer_id ON bank_account(customer_id);
CREATE INDEX idx_accounts_customer_status ON bank_account(customer_id, status);
CREATE INDEX idx_accounts_customer_primary ON bank_account(customer_id, is_primary);

-- =====================================================
-- customer_device TABLE
-- =====================================================
CREATE TABLE customer_device (
    device_identifier VARCHAR(100) PRIMARY KEY CHECK (LENGTH(TRIM(device_identifier)) > 0 AND device_identifier ~ '^(IMEI|MAC|UUID|ANDROID_ID):[A-Za-z0-9:-]+$'),
    customer_id UUID NOT NULL,
    
    -- Basic identification
    device_type VARCHAR(50) NOT NULL CHECK (device_type IN ('Mobile', 'Desktop', 'Tablet')),
    device_name VARCHAR(255),
    
    -- Trust management
    is_trusted BOOLEAN DEFAULT FALSE,
    status VARCHAR(20) DEFAULT 'Active' CHECK (status IN ('Active', 'Blocked', 'Expired')),
    
    -- Basic tracking
    first_seen_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    last_used_at TIMESTAMPTZ
);

-- customer_device CONSTRAINTS
ALTER TABLE customer_device ADD CONSTRAINT fk_customer_device_customer 
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id) 
    ON DELETE CASCADE;

-- customer_device INDEXES
CREATE INDEX idx_devices_customer_status ON customer_device(customer_id, status);
CREATE INDEX idx_devices_customer_trusted ON customer_device(customer_id, is_trusted);

-- =====================================================
-- transaction TABLE
-- =====================================================
CREATE TABLE transaction (
    transaction_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID NOT NULL,
    transaction_type VARCHAR(50) NOT NULL CHECK (transaction_type IN ('Internal_Transfer', 'External_Transfer', 'Bill_Payment')),
    amount DECIMAL(15,2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) DEFAULT 'VND' CHECK (currency IN ('VND', 'USD', 'EUR')),
    fee DECIMAL(15,2) DEFAULT 0.00 CHECK (fee >= 0),
    status VARCHAR(20) DEFAULT 'Pending' CHECK (status IN ('Pending', 'Processing', 'Completed', 'Failed', 'Cancelled')),
    note TEXT,
    authentication_method VARCHAR(50) CHECK (authentication_method IN ('PIN', 'PIN_OTP', 'PIN_OTP_Biometric')),
    
    -- RECIPIENT INFORMATION (Both Internal & External)
    recipient_account_number VARCHAR(50),
    recipient_bank_code VARCHAR(20),
    recipient_name VARCHAR(255),
    
    -- BILL PAYMENT FIELDS
    service_provider_code VARCHAR(20),
    bill_number VARCHAR(100),
    
    -- FRAUD DETECTION FIELDS
    is_fraud BOOLEAN DEFAULT FALSE,
    fraud_score DECIMAL(5,2) DEFAULT 0.00 CHECK (fraud_score BETWEEN 0 AND 100),
    
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ
);

-- transaction CONSTRAINTS
ALTER TABLE transaction ADD CONSTRAINT fk_transaction_account 
    FOREIGN KEY (account_id) REFERENCES bank_account(account_id) 
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE transaction ADD CONSTRAINT chk_internal_transfer_fields 
    CHECK (
        (transaction_type = 'Internal_Transfer' AND 
         recipient_account_number IS NOT NULL AND recipient_bank_code IS NULL AND
         service_provider_code IS NULL AND bill_number IS NULL) OR
        (transaction_type != 'Internal_Transfer')
    );

ALTER TABLE transaction ADD CONSTRAINT chk_external_transfer_fields 
    CHECK (
        (transaction_type = 'External_Transfer' AND 
         recipient_account_number IS NOT NULL AND recipient_bank_code IS NOT NULL AND 
         service_provider_code IS NULL AND bill_number IS NULL) OR
        (transaction_type != 'External_Transfer')
    );

ALTER TABLE transaction ADD CONSTRAINT chk_bill_payment_fields 
    CHECK (
        (transaction_type = 'Bill_Payment' AND 
         service_provider_code IS NOT NULL AND bill_number IS NOT NULL AND
         recipient_account_number IS NULL AND recipient_bank_code IS NULL) OR
        (transaction_type != 'Bill_Payment')
    );

ALTER TABLE transaction ADD CONSTRAINT chk_amount_fee_relationship 
    CHECK (fee <= amount * 0.1);

-- transaction INDEXES
CREATE INDEX idx_transaction_account_status ON transaction(account_id, status);
CREATE INDEX idx_transaction_account_type ON transaction(account_id, transaction_type);
CREATE INDEX idx_transaction_account_date ON transaction(account_id, created_at);
CREATE INDEX idx_transaction_recipient_account ON transaction(recipient_account_number);
CREATE INDEX idx_transaction_service_provider ON transaction(service_provider_code);

-- =====================================================
-- authentication_log TABLE
-- =====================================================
CREATE TABLE authentication_log (
    log_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id UUID NOT NULL,
    device_identifier VARCHAR(100),
    authentication_type VARCHAR(50) NOT NULL CHECK (authentication_type IN (
        'Login_Password', 'Login_Biometric', 'Transaction_PIN', 
        'Transaction_OTP', 'Transaction_Biometric'
    )),
    transaction_id UUID,
    ip_address VARCHAR(45),
    status VARCHAR(20) NOT NULL CHECK (status IN ('Success', 'Failed', 'Blocked', 'Timeout')),
    failure_reason VARCHAR(255),
    otp_sent_to VARCHAR(50),
    biometric_score DECIMAL(5,4),
    attempt_count INT DEFAULT 1,
    session_id VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- authentication_log CONSTRAINTS
ALTER TABLE authentication_log ADD CONSTRAINT fk_auth_logs_customer 
    FOREIGN KEY (customer_id) REFERENCES customer(customer_id) 
    ON DELETE CASCADE;

ALTER TABLE authentication_log ADD CONSTRAINT fk_auth_logs_transaction 
    FOREIGN KEY (transaction_id) REFERENCES transaction(transaction_id) 
    ON DELETE SET NULL;

ALTER TABLE authentication_log ADD CONSTRAINT fk_auth_logs_device 
    FOREIGN KEY (device_identifier) REFERENCES customer_device(device_identifier) 
    ON DELETE SET NULL;

-- authentication_log INDEXES
CREATE INDEX idx_auth_logs_customer_status ON authentication_log(customer_id, status);
CREATE INDEX idx_auth_logs_customer_date ON authentication_log(customer_id, created_at);
CREATE INDEX idx_auth_logs_device_identifier ON authentication_log(device_identifier);
CREATE INDEX idx_auth_logs_transaction_id ON authentication_log(transaction_id);
CREATE INDEX idx_auth_logs_session_id ON authentication_log(session_id);