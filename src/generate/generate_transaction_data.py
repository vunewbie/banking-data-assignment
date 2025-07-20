import hashlib
import random
import uuid
from datetime import datetime, timedelta

# =========================
# "transaction_type" data
# =========================
def generate_transaction_type():
    """
    Generate transaction type with realistic distribution for Vietnamese banking
    
    Returns:
        str: Transaction type
    """
    
    # Based on Vietnamese banking transaction patterns
    transaction_types = ['Transfer', 'Payment', 'Deposit', 'Withdrawal']
    weights = [0.45, 0.30, 0.15, 0.10]  # Transfer and Payment most common
    
    return random.choices(transaction_types, weights=weights)[0]

# =========================
# "transaction_amount" data
# =========================
def generate_transaction_amount(transaction_type, customer_income=10_000_000):
    """
    Generate realistic transaction amount based on type and customer income
    
    Args:
        transaction_type (str): Type of transaction
        customer_income (int): Customer monthly income in VND
        
    Returns:
        int: Transaction amount in VND
    """
    
    # Base amount ranges by transaction type (VND)
    amount_ranges = {
        'Transfer': {
            'small': (50_000, 2_000_000),      # 50K - 2M: 70%
            'medium': (2_000_000, 10_000_000), # 2M - 10M: 25%
            'large': (10_000_000, 50_000_000)  # 10M - 50M: 5%
        },
        'Payment': {
            'small': (20_000, 1_000_000),      # 20K - 1M: 80%
            'medium': (1_000_000, 5_000_000),  # 1M - 5M: 18%
            'large': (5_000_000, 20_000_000)   # 5M - 20M: 2%
        },
        'Deposit': {
            'small': (100_000, 5_000_000),     # 100K - 5M: 60%
            'medium': (5_000_000, 20_000_000), # 5M - 20M: 35%
            'large': (20_000_000, 100_000_000) # 20M - 100M: 5%
        },
        'Withdrawal': {
            'small': (50_000, 2_000_000),      # 50K - 2M: 85%
            'medium': (2_000_000, 5_000_000),  # 2M - 5M: 14%
            'large': (5_000_000, 10_000_000)   # 5M - 10M: 1%
        }
    }
    
    # Select amount category based on transaction type
    if transaction_type == 'Transfer':
        categories = ['small', 'medium', 'large']
        weights = [0.70, 0.25, 0.05]
    elif transaction_type == 'Payment':
        categories = ['small', 'medium', 'large']
        weights = [0.80, 0.18, 0.02]
    elif transaction_type == 'Deposit':
        categories = ['small', 'medium', 'large']
        weights = [0.60, 0.35, 0.05]
    else:  # Withdrawal
        categories = ['small', 'medium', 'large']
        weights = [0.85, 0.14, 0.01]
    
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
# "transaction_description" data
# ===============================
def generate_transaction_description(transaction_type, amount):
    """
    Generate realistic transaction description
    
    Args:
        transaction_type (str): Type of transaction
        amount (int): Transaction amount
        
    Returns:
        str: Transaction description
    """
    
    descriptions = {
        'Transfer': [
            'Chuyển tiền cá nhân', 'Chuyển tiền gia đình', 'Trả nợ bạn bè',
            'Hỗ trợ gia đình', 'Chuyển tiền khẩn cấp', 'Chia sẻ chi phí'
        ],
        'Payment': [
            'Thanh toán hóa đơn điện', 'Thanh toán hóa đơn nước', 'Thanh toán internet',
            'Thanh toán điện thoại', 'Mua sắm online', 'Thanh toán bảo hiểm',
            'Học phí', 'Phí dịch vụ'
        ],
        'Deposit': [
            'Nộp tiền mặt', 'Chuyển khoản từ ngân hàng khác', 'Tiền lương',
            'Thu nhập khác', 'Hoàn tiền', 'Tiền thưởng'
        ],
        'Withdrawal': [
            'Rút tiền mặt ATM', 'Rút tiền tại quầy', 'Chi tiêu cá nhân',
            'Rút tiền khẩn cấp'
        ]
    }
    
    base_desc = random.choice(descriptions[transaction_type])
    
    # Add amount-specific details for large transactions
    if amount >= 10_000_000:
        if transaction_type == 'Transfer':
            base_desc += ' (giao dịch lớn)'
        elif transaction_type == 'Payment':
            base_desc += ' (thanh toán lớn)'
    
    return base_desc

# ===============================
# "authentication_method" data
# ===============================
def determine_authentication_method(amount, device_trusted=True):
    """
    Determine required authentication method based on amount and device trust
    
    Args:
        amount (int): Transaction amount in VND
        device_trusted (bool): Whether device is trusted
        
    Returns:
        list: Required authentication methods
    """
    
    auth_methods = []
    
    # Base authentication (always required)
    auth_methods.append('PIN')
    
    # Amount-based authentication requirements
    if amount >= 10_000_000:
        # Transactions ≥10M VND require strong authentication
        strong_auth_options = ['Biometric', 'OTP']
        auth_methods.append(random.choice(strong_auth_options))
    elif amount >= 5_000_000:
        # Transactions 5M-10M VND sometimes require additional auth
        if random.random() < 0.3:  # 30% chance
            auth_methods.append('OTP')
    
    # Device-based authentication
    if not device_trusted:
        # Untrusted devices require additional verification
        if 'OTP' not in auth_methods:
            auth_methods.append('OTP')
    
    return auth_methods

# ===============================
# "transaction_status" data
# ===============================
def generate_transaction_status(auth_methods, has_biometric=True):
    """
    Generate transaction status based on authentication requirements
    
    Args:
        auth_methods (list): Required authentication methods
        has_biometric (bool): Whether customer has biometric setup
        
    Returns:
        str: Transaction status
    """
    
    # Check if all required auth methods are available
    if 'Biometric' in auth_methods and not has_biometric:
        # Customer doesn't have biometric setup, might fail
        if random.random() < 0.3:  # 30% failure rate
            return 'Failed'
    
    # Most transactions succeed if auth requirements are met
    statuses = ['Completed', 'Failed', 'Pending']
    weights = [0.92, 0.06, 0.02]  # 92% success, 6% failed, 2% pending
    
    return random.choices(statuses, weights=weights)[0]

# ===============================
# "recipient_account", "recipient_name", "recipient_bank" data
# ===============================
def generate_recipient_info(transaction_type):
    """
    Generate recipient information for transactions
    
    Args:
        transaction_type (str): Type of transaction
        
    Returns:
        dict: Recipient information
    """
    
    if transaction_type in ['Deposit', 'Withdrawal']:
        # Internal transactions - no external recipient
        return {
            'recipient_account': None,
            'recipient_name': None,
            'recipient_bank': 'BVBank'
        }
    
    # External recipients for Transfer/Payment
    vietnamese_names = [
        'Nguyễn Văn An', 'Trần Thị Bình', 'Lê Hoàng Nam', 'Phạm Thị Lan',
        'Hoàng Văn Dũng', 'Võ Thị Mai', 'Đặng Minh Khang', 'Bùi Thị Hương'
    ]
    
    # Bank list for transfers
    banks = {
        'BVBank': 0.3,  # Same bank transfers
        'Vietcombank': 0.15,
        'BIDV': 0.12,
        'VietinBank': 0.10,
        'Agribank': 0.10,
        'Techcombank': 0.08,
        'MBBank': 0.06,
        'VPBank': 0.05,
        'Others': 0.04
    }
    
    recipient_bank = random.choices(list(banks.keys()), weights=list(banks.values()))[0]
    
    # Generate account number based on bank
    if recipient_bank == 'BVBank':
        # Same bank - BVBank format
        account_number = '280' + ''.join([str(random.randint(0, 9)) for _ in range(15)])
    else:
        # External bank - generic format
        account_number = ''.join([str(random.randint(0, 9)) for _ in range(random.choice([10, 12, 14]))])
    
    return {
        'recipient_account': account_number,
        'recipient_name': random.choice(vietnamese_names),
        'recipient_bank': recipient_bank
    }


def reset_transaction_tracking():
    """
    Reset transaction tracking (useful for testing or new generation sessions)
    """
    pass  # Currently no global tracking needed, but placeholder for future use 