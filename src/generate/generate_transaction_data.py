import hashlib
import random
import uuid
from datetime import datetime, timedelta

# =========================
# "transaction_type" data - FIXED TO MATCH SCHEMA
# =========================
def generate_transaction_type():
    """
    Generate transaction type matching schema.sql exactly
    Schema: CHECK (transaction_type IN ('Internal_Transfer', 'External_Transfer', 'Bill_Payment'))
    
    Returns:
        str: Transaction type from schema
    """
    
    # Based on Vietnamese banking transaction patterns - CORRECTED TYPES
    transaction_types = ['Internal_Transfer', 'External_Transfer', 'Bill_Payment']
    weights = [0.45, 0.35, 0.20]  # Internal transfer most common, then external, then bill payment
    
    return random.choices(transaction_types, weights=weights)[0]

# =========================
# "amount" data
# =========================
def generate_transaction_amount(transaction_type, customer_income=10_000_000):
    """
    Generate realistic transaction amount based on type and customer income
    
    Args:
        transaction_type (str): Type of transaction (Internal_Transfer, External_Transfer, Bill_Payment)
        customer_income (int): Customer monthly income in VND
        
    Returns:
        int: Transaction amount in VND
    """
    
    # Base amount ranges by transaction type (VND) - UPDATED FOR SCHEMA TYPES
    amount_ranges = {
        'Internal_Transfer': {
            'small': (50_000, 2_000_000),      # 50K - 2M: 70%
            'medium': (2_000_000, 10_000_000), # 2M - 10M: 25%
            'large': (10_000_000, 50_000_000)  # 10M - 50M: 5%
        },
        'External_Transfer': {
            'small': (100_000, 5_000_000),     # 100K - 5M: 60%
            'medium': (5_000_000, 20_000_000), # 5M - 20M: 35%
            'large': (20_000_000, 100_000_000) # 20M - 100M: 5%
        },
        'Bill_Payment': {
            'small': (20_000, 1_000_000),      # 20K - 1M: 80%
            'medium': (1_000_000, 5_000_000),  # 1M - 5M: 18%
            'large': (5_000_000, 20_000_000)   # 5M - 20M: 2%
        }
    }
    
    # Select amount category based on transaction type
    if transaction_type == 'Internal_Transfer':
        categories = ['small', 'medium', 'large']
        weights = [0.70, 0.25, 0.05]
    elif transaction_type == 'External_Transfer':
        categories = ['small', 'medium', 'large']
        weights = [0.60, 0.35, 0.05]
    else:  # Bill_Payment
        categories = ['small', 'medium', 'large']
        weights = [0.80, 0.18, 0.02]
    
    category = random.choices(categories, weights=weights)[0]
    min_amount, max_amount = amount_ranges[transaction_type][category]
    
    # Adjust based on customer income
    income_multiplier = min(customer_income / 10_000_000, 3.0)  # Cap at 3x
    adjusted_max_amount = int(max_amount * income_multiplier)
    
    # Ensure min_amount <= max_amount
    final_min_amount = min_amount
    final_max_amount = max(adjusted_max_amount, min_amount)
    
    # Generate amount
    amount = random.randint(final_min_amount, final_max_amount)
    
    # Round to nearest 10K for realism
    amount = round(amount / 10_000) * 10_000
    
    return amount

# ===============================
# "currency" data - ADDED MISSING FIELD
# ===============================
def generate_transaction_currency():
    """
    Generate currency with realistic distribution for transactions
    Schema: CHECK (currency IN ('VND', 'USD', 'EUR'))
    
    Returns:
        str: Currency code
    """
    
    # Vietnamese banking: mostly VND
    currencies = ['VND', 'USD', 'EUR']
    weights = [0.85, 0.12, 0.03]  # 85% VND, 12% USD, 3% EUR
    
    return random.choices(currencies, weights=weights)[0]

# ===============================
# "fee" data - FIXED FIELD NAME
# ===============================
def generate_fee(transaction_type, amount):
    """
    Generate transaction fee based on type and amount
    Schema: fee DECIMAL(15,2) DEFAULT 0.00 CHECK (fee >= 0)
    
    Args:
        transaction_type (str): Transaction type
        amount (int): Transaction amount
        
    Returns:
        float: Fee amount
    """
    
    if transaction_type == 'Internal_Transfer':
        # Same bank transfers - usually free or very low fee
        if amount < 5_000_000:
            return 0.0  # Free for small amounts
        else:
            return round(random.uniform(1_000, 5_000), 2)  # 1K-5K VND
    
    elif transaction_type == 'External_Transfer':
        # External transfers - percentage fee (max 10% constraint)
        fee_rate = random.uniform(0.001, 0.005)  # 0.1-0.5%
        fee_amount = amount * fee_rate
        # Min 5K, max 50K VND, but never exceed 10% of amount
        max_allowed_fee = amount * 0.1  # 10% constraint
        fee_amount = max(5_000, min(fee_amount, 50_000))
        fee_amount = min(fee_amount, max_allowed_fee)  # Ensure constraint compliance
        return round(fee_amount, 2)
    
    else:  # Bill_Payment
        # Bill payments - flat fee (max 10% constraint)
        base_fee = random.uniform(1_000, 3_000)  # 1K-3K VND
        max_allowed_fee = amount * 0.1  # 10% constraint
        return round(min(base_fee, max_allowed_fee), 2)

# ===============================
# "note" data - FIXED FIELD NAME
# ===============================
def generate_note(transaction_type, amount):
    """
    Generate transaction note (was description)
    Schema: note TEXT
    
    Args:
        transaction_type (str): Type of transaction
        amount (int): Transaction amount
        
    Returns:
        str: Transaction note
    """
    
    notes = {
        'Internal_Transfer': [
            'Chuyển tiền cá nhân', 'Chuyển tiền gia đình', 'Trả nợ bạn bè',
            'Hỗ trợ gia đình', 'Chuyển tiền khẩn cấp', 'Chia sẻ chi phí',
            'Chuyển khoản nội bộ', 'Hỗ trợ tài chính'
        ],
        'External_Transfer': [
            'Chuyển tiền ngân hàng khác', 'Thanh toán người bán', 'Đầu tư',
            'Mua bán hàng hóa', 'Thanh toán dịch vụ', 'Kinh doanh',
            'Hợp tác đối tác', 'Giao dịch thương mại'
        ],
        'Bill_Payment': [
            'Thanh toán hóa đơn điện', 'Thanh toán hóa đơn nước', 'Thanh toán internet',
            'Thanh toán điện thoại', 'Thanh toán gas', 'Thanh toán bảo hiểm',
            'Học phí', 'Phí dịch vụ', 'Thanh toán truyền hình'
        ]
    }
    
    base_note = random.choice(notes[transaction_type])
    
    # Add amount-specific details for large transactions
    if amount >= 10_000_000:
        base_note += ' (giao dịch lớn)'
    
    return base_note

# ===============================
# "authentication_method" data - ADDED MISSING FIELD
# ===============================
def generate_authentication_method(amount, device_trusted=True):
    """
    Generate authentication method based on amount and device trust
    Schema: CHECK (authentication_method IN ('PIN', 'PIN_OTP', 'PIN_OTP_Biometric'))
    
    Args:
        amount (int): Transaction amount in VND
        device_trusted (bool): Whether device is trusted
        
    Returns:
        str: Authentication method from schema
    """
    
    # Base authentication based on amount (compliance with 2345/QĐ-NHNN 2023)
    if amount >= 10_000_000:
        # Transactions ≥10M VND require strong authentication
        methods = ['PIN_OTP', 'PIN_OTP_Biometric']
        weights = [0.7, 0.3]  # 70% OTP, 30% Biometric
        return random.choices(methods, weights=weights)[0]
    
    elif amount >= 5_000_000:
        # 5M-10M VND: mix of authentication methods
        if device_trusted:
            methods = ['PIN', 'PIN_OTP', 'PIN_OTP_Biometric']
            weights = [0.6, 0.3, 0.1]
        else:
            # Untrusted devices need stronger auth
            methods = ['PIN_OTP', 'PIN_OTP_Biometric']
            weights = [0.8, 0.2]
        return random.choices(methods, weights=weights)[0]
    
    else:
        # Small amounts: mostly PIN, some OTP for untrusted devices
        if device_trusted:
            methods = ['PIN', 'PIN_OTP']
            weights = [0.9, 0.1]
        else:
            methods = ['PIN', 'PIN_OTP']
            weights = [0.7, 0.3]
        return random.choices(methods, weights=weights)[0]

# ===============================
# "recipient_account_number", "recipient_bank_code", "recipient_name" data - FIXED
# ===============================
def generate_recipient_info(transaction_type):
    """
    Generate recipient information based on transaction type and schema constraints
    
    Args:
        transaction_type (str): Type of transaction
        
    Returns:
        dict: Recipient information matching schema constraints
    """
    
    vietnamese_names = [
        'Nguyễn Văn An', 'Trần Thị Bình', 'Lê Hoàng Nam', 'Phạm Thị Lan',
        'Hoàng Văn Dũng', 'Võ Thị Mai', 'Đặng Minh Khang', 'Bùi Thị Hương',
        'Lý Quang Huy', 'Tôn Thị Nga', 'Phan Minh Tuấn', 'Chu Thị Linh'
    ]
    
    if transaction_type == 'Internal_Transfer':
        # Schema constraint: recipient_account_number IS NOT NULL AND recipient_bank_code IS NULL
        # Same bank transfer - BVBank account format
        account_number = '280' + ''.join([str(random.randint(0, 9)) for _ in range(15)])
        return {
            'recipient_account_number': account_number,
            'recipient_bank_code': None,  # NULL for internal transfers
            'recipient_name': random.choice(vietnamese_names)
        }
    
    elif transaction_type == 'External_Transfer':
        # Schema constraint: recipient_account_number IS NOT NULL AND recipient_bank_code IS NOT NULL
        # External bank transfer
        bank_codes = {
            'VCB': 'Vietcombank',
            'BID': 'BIDV', 
            'VTB': 'VietinBank',
            'AGR': 'Agribank',
            'TCB': 'Techcombank',
            'MBB': 'MBBank',
            'VPB': 'VPBank',
            'STB': 'Sacombank'
        }
        
        bank_code = random.choice(list(bank_codes.keys()))
        # Generate account number for external bank (various formats)
        account_number = ''.join([str(random.randint(0, 9)) for _ in range(random.choice([10, 12, 14]))])
        
        return {
            'recipient_account_number': account_number,
            'recipient_bank_code': bank_code,
            'recipient_name': random.choice(vietnamese_names)
        }
    
    else:  # Bill_Payment
        # Schema constraint: recipient_account_number IS NULL AND recipient_bank_code IS NULL
        return {
            'recipient_account_number': None,
            'recipient_bank_code': None,
            'recipient_name': None  # Bills don't have recipient names
        }

# ===============================
# "service_provider_code", "bill_number" data - ADDED MISSING FIELDS
# ===============================
def generate_bill_payment_info(transaction_type):
    """
    Generate bill payment specific information
    Schema constraint for Bill_Payment: service_provider_code IS NOT NULL AND bill_number IS NOT NULL
    
    Args:
        transaction_type (str): Transaction type
        
    Returns:
        dict: Bill payment information
    """
    
    if transaction_type != 'Bill_Payment':
        return {
            'service_provider_code': None,
            'bill_number': None
        }
    
    # Vietnamese service providers
    service_providers = {
        'EVN': 'Electricity (Vietnam Electricity)',
        'SAWACO': 'Water (Saigon Water Corporation)', 
        'VTV': 'Television (Vietnam Television)',
        'VNPT': 'Internet/Phone (VNPT)',
        'VIETTEL': 'Mobile (Viettel)',
        'MOBIFONE': 'Mobile (MobiFone)',
        'VINAPHONE': 'Mobile (VinaPhone)',
        'PV_GAS': 'Gas (PetroVietnam Gas)',
        'BH_SOCIAL': 'Social Insurance',
        'BH_XH': 'Health Insurance'
    }
    
    provider_code = random.choice(list(service_providers.keys()))
    
    # Generate bill number based on provider
    if provider_code in ['EVN', 'SAWACO']:
        # Utility bills: customer code + month/year
        bill_number = f"{random.randint(100000, 999999)}{random.randint(1, 12):02d}{random.randint(2024, 2024)}"
    elif provider_code in ['VIETTEL', 'MOBIFONE', 'VINAPHONE']:
        # Mobile bills: phone number
        prefixes = ['096', '097', '098', '032', '033', '034', '035', '036', '037', '038', '039']
        bill_number = random.choice(prefixes) + ''.join([str(random.randint(0, 9)) for _ in range(7)])
    else:
        # Other bills: random format
        bill_number = f"{random.randint(10000000, 99999999)}"
    
    return {
        'service_provider_code': provider_code,
        'bill_number': bill_number
    }

# ===============================
# "status" data
# ===============================
def generate_transaction_status(auth_method, has_biometric=True):
    """
    Generate transaction status based on authentication method
    Schema: CHECK (status IN ('Pending', 'Processing', 'Completed', 'Failed', 'Cancelled'))
    
    Args:
        auth_method (str): Authentication method used
        has_biometric (bool): Whether customer has biometric setup
        
    Returns:
        str: Transaction status
    """
    
    # Check if biometric auth is possible
    if 'Biometric' in auth_method and not has_biometric:
        # Customer doesn't have biometric setup, likely to fail
        statuses = ['Failed', 'Completed', 'Cancelled']
        weights = [0.6, 0.3, 0.1]
        return random.choices(statuses, weights=weights)[0]
    
    # Most transactions succeed
    statuses = ['Completed', 'Failed', 'Pending', 'Processing', 'Cancelled']
    weights = [0.88, 0.07, 0.03, 0.015, 0.005]  # 88% success rate
    
    return random.choices(statuses, weights=weights)[0]

# ===============================
# "is_fraud", "fraud_score" data - ADDED MISSING FIELDS
# ===============================
def generate_fraud_detection_info(amount, auth_method, device_trusted):
    """
    Generate fraud detection information
    Schema: is_fraud BOOLEAN DEFAULT FALSE, fraud_score DECIMAL(5,2) DEFAULT 0.00 CHECK (fraud_score BETWEEN 0 AND 100)
    
    Args:
        amount (int): Transaction amount
        auth_method (str): Authentication method
        device_trusted (bool): Device trust status
        
    Returns:
        dict: Fraud detection info
    """
    
    # Base fraud probability (very low for banking data)
    base_fraud_prob = 0.005  # 0.5% base fraud rate
    
    # Risk factors
    risk_multiplier = 1.0
    
    # High amount increases risk
    if amount >= 50_000_000:
        risk_multiplier *= 3.0
    elif amount >= 20_000_000:
        risk_multiplier *= 2.0
    elif amount >= 10_000_000:
        risk_multiplier *= 1.5
    
    # Untrusted device increases risk
    if not device_trusted:
        risk_multiplier *= 2.0
    
    # Weak authentication increases risk
    if auth_method == 'PIN':
        risk_multiplier *= 1.5
    
    final_fraud_prob = min(base_fraud_prob * risk_multiplier, 0.05)  # Cap at 5%
    is_fraud = random.random() < final_fraud_prob
    
    if is_fraud:
        # Fraudulent transactions have higher fraud scores
        fraud_score = round(random.uniform(70.0, 95.0), 2)
    else:
        # Normal transactions have low fraud scores
        fraud_score = round(random.uniform(0.0, 15.0), 2)
    
    return {
        'is_fraud': is_fraud,
        'fraud_score': fraud_score
    }

# ===============================
# "completed_at" data - ADDED MISSING FIELD
# ===============================
def generate_completed_at(created_at, status):
    """
    Generate completed_at timestamp based on status
    Schema: completed_at TIMESTAMPTZ
    
    Args:
        created_at (datetime): Transaction creation time
        status (str): Transaction status
        
    Returns:
        datetime or None: Completion timestamp
    """
    
    if status not in ['Completed', 'Failed', 'Cancelled']:
        return None  # Pending/Processing transactions not completed yet
    
    # Completed transactions: 1 minute to 2 hours after creation
    completion_delay_minutes = random.randint(1, 120)
    return created_at + timedelta(minutes=completion_delay_minutes)

def reset_transaction_tracking():
    """
    Reset transaction tracking (useful for testing or new generation sessions)
    """
    pass  # Currently no global tracking needed, but placeholder for future use 