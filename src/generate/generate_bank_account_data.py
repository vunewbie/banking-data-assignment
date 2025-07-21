import hashlib
import random

#
# =====================================================================================
# "account_number" data
# =====================================================================================
# Track used account numbers to ensure uniqueness
_used_account_numbers = set()

def generate_account_number(customer_id, account_type='Savings'):
    """
    Generate BVBank (Timo) account number with realistic Vietnamese format
    
    BVBank account number format: 280xxxxxxxxxxxxxxx (18 digits total)
    - 280: BVBank identifier (fixed prefix)
    - xxxxxxxxxxxxxxx: 15-digit unique sequential number
    
    Args:
        customer_id (str): Customer UUID for generating unique number
        account_type (str): Account type ('Savings', 'Current', 'Fixed_Deposit', 'Loan')
        
    Returns:
        str: 18-digit BVBank account number
    """
    max_attempts = 1000  # Prevent infinite loop
    attempts = 0
    
    while attempts < max_attempts:
        # BVBank prefix (fixed)
        bank_prefix = '280'
        
        # Generate 15-digit unique number based on customer_id and account_type
        # Use customer_id + account_type as seed for uniqueness
        seed_string = f"{customer_id}_{account_type}_{attempts}"
        seed_value = int(hashlib.md5(seed_string.encode()).hexdigest()[:12], 16)
        
        # Generate 15-digit sequential number
        # Ensure it starts with non-zero to maintain 15 digits
        sequential_number = seed_value % 900000000000000 + 100000000000000
        
        # Combine to form full account number
        account_number = f"{bank_prefix}{sequential_number:015d}"
        
        # Ensure uniqueness
        if account_number not in _used_account_numbers:
            _used_account_numbers.add(account_number)
            return account_number
        
        attempts += 1
    
    # Fallback if can't generate unique number
    raise Exception(f"Could not generate unique BVBank account number after {max_attempts} attempts")


def reset_account_number_tracking():
    """
    Reset account number tracking (useful for testing or new generation sessions)
    """
    global _used_account_numbers
    _used_account_numbers.clear()


# =====================
# "account_type" data
# =====================
def generate_account_type():
    """
    Generate account type with realistic distribution for Vietnamese banking
    Schema: CHECK (account_type IN ('Savings', 'Current', 'Fixed_Deposit', 'Loan'))
    
    Returns:
        str: Account type - 'Savings', 'Current', 'Fixed_Deposit', 'Loan'
    """
    
    # Realistic distribution based on Vietnamese banking patterns
    account_types = ['Savings', 'Current', 'Fixed_Deposit', 'Loan']
    weights = [0.65, 0.20, 0.10, 0.05]  # 65% Savings, 20% Current, 10% Fixed_Deposit, 5% Loan
    
    return random.choices(account_types, weights=weights)[0]

# =====================
# "currency" data
# =====================
def generate_account_currency(customer_type='Individual'):
    """
    Generate currency with realistic distribution for bank accounts
    Schema: CHECK (currency IN ('VND', 'USD', 'EUR'))
    
    Args:
        customer_type (str): 'Individual' or 'Organization'
        
    Returns:
        str: Currency code - 'VND', 'USD', 'EUR'
    """
    
    if customer_type == 'Organization':
        # Organizations more likely to have foreign currency accounts
        currencies = ['VND', 'USD', 'EUR']
        weights = [0.75, 0.20, 0.05]  # 75% VND, 20% USD, 5% EUR
    else:
        # Individuals mostly use VND
        currencies = ['VND', 'USD', 'EUR']
        weights = [0.90, 0.08, 0.02]  # 90% VND, 8% USD, 2% EUR
    
    return random.choices(currencies, weights=weights)[0]

# ====================================
# "available_balance", "current_balance", "hold_amount" data - FIXED TO MATCH SCHEMA
# ====================================
def generate_balance_info(account_type, customer_income):
    """
    Generate balance information according to schema constraints
    Schema: 
    - available_balance DECIMAL(15,2) DEFAULT 0.00 CHECK (available_balance >= 0)
    - current_balance DECIMAL(15,2) DEFAULT 0.00 CHECK (current_balance >= 0)  
    - hold_amount DECIMAL(15,2) DEFAULT 0.00 CHECK (hold_amount >= 0)
    - CHECK (current_balance = available_balance + hold_amount)
    
    Args:
        account_type (str): Account type
        customer_income (float): Customer monthly income
        
    Returns:
        dict: Balance information with available_balance, current_balance, hold_amount
    """
    
    # Generate available balance based on account type and income
    if account_type == 'Savings':
        # Savings: 1-6 months of income
        balance_multiplier = random.uniform(1, 6)
    elif account_type == 'Current':
        # Current: 0.5-3 months of income
        balance_multiplier = random.uniform(0.5, 3)
    elif account_type == 'Fixed_Deposit':
        # Fixed deposit: 3-12 months of income
        balance_multiplier = random.uniform(3, 12)
    else:  # Loan
        # Loan accounts: typically negative available but showing as 0 available
        balance_multiplier = random.uniform(0.1, 1.0)
    
    available_balance = round(customer_income * balance_multiplier, 2)
    # Round to nearest 100K for realism
    available_balance = round(available_balance / 100_000) * 100_000
    
    # Generate hold amount (5% of accounts have holds)
    if random.random() < 0.05:
        # Some accounts have holds - typically 1-10% of available balance
        hold_percentage = random.uniform(0.01, 0.10)
        hold_amount = round(available_balance * hold_percentage, 2)
        # Round to nearest 10K
        hold_amount = round(hold_amount / 10_000) * 10_000
    else:
        hold_amount = 0.0
    
    # Calculate current balance (must equal available + hold per schema constraint)
    current_balance = available_balance + hold_amount
    
    return {
        'available_balance': available_balance,
        'current_balance': current_balance,
        'hold_amount': hold_amount
    }

# ============================
# "daily_transfer_limit" data
# ============================
def generate_daily_transfer_limit(account_type, customer_type='Individual'):
    """
    Generate realistic daily transfer limit based on account type and customer type
    Schema: daily_transfer_limit DECIMAL(15,2) DEFAULT 50000000.00 CHECK (daily_transfer_limit > 0)
    
    Args:
        account_type (str): Account type
        customer_type (str): 'Individual' or 'Organization'
        
    Returns:
        float: Daily transfer limit in VND
    """
    
    # Base limits by account type (VND)
    base_limits = {
        'Savings': (20_000_000, 100_000_000),     # 20M - 100M VND
        'Current': (50_000_000, 200_000_000),     # 50M - 200M VND  
        'Fixed_Deposit': (10_000_000, 50_000_000), # 10M - 50M VND (lower for term deposits)
        'Loan': (100_000_000, 500_000_000)       # 100M - 500M VND (higher for loan accounts)
    }
    
    min_limit, max_limit = base_limits.get(account_type, (50_000_000, 150_000_000))
    
    # Customer type multiplier
    if customer_type == 'Organization':
        # Organizations typically have higher limits
        min_limit *= 2
        max_limit *= 3
    
    # Generate random limit within range
    limit = random.randint(min_limit, max_limit)
    
    # Round to nearest million for realism
    limit = round(limit / 1_000_000) * 1_000_000
    
    return float(limit)

# ==================================
# "daily_online_payment_limit" data
# ==================================
def generate_daily_online_payment_limit(daily_transfer_limit):
    """
    Generate daily online payment limit (typically lower than transfer limit)
    Schema: daily_online_payment_limit DECIMAL(15,2) DEFAULT 20000000.00 CHECK (daily_online_payment_limit > 0)
    
    Args:
        daily_transfer_limit (float): Daily transfer limit
        
    Returns:
        float: Daily online payment limit in VND
    """
    
    # Online payment limit is typically 40-70% of transfer limit
    percentage = random.uniform(0.4, 0.7)
    payment_limit = daily_transfer_limit * percentage
    
    # Round to nearest 100K for realism
    payment_limit = round(payment_limit / 100_000) * 100_000
    
    # Minimum 5M VND per schema constraint (> 0)
    payment_limit = max(payment_limit, 5_000_000)
    
    return float(payment_limit)

# ======================
# "is_primary" data
# ======================
def generate_is_primary(account_number_in_sequence=0):
    """
    Generate is_primary flag (only one primary account per customer)
    Schema: is_primary BOOLEAN DEFAULT FALSE
    
    Args:
        account_number_in_sequence (int): 0 for first account, 1+ for additional accounts
        
    Returns:
        bool: True if this is primary account, False otherwise
    """
    
    if account_number_in_sequence == 0:
        # First account is usually primary
        return random.random() < 0.9  # 90% chance first account is primary
    else:
        # Additional accounts are rarely primary
        return random.random() < 0.1  # 10% chance additional account is primary

# ======================
# "status" data
# ======================
def generate_account_status():
    """
    Generate account status with realistic distribution
    Schema: CHECK (status IN ('Active', 'Inactive', 'Suspended', 'Closed'))
    
    Returns:
        str: Account status - 'Active', 'Inactive', 'Suspended', 'Closed'
    """
    
    # Most accounts are active
    statuses = ['Active', 'Inactive', 'Suspended', 'Closed']
    weights = [0.88, 0.07, 0.03, 0.02]  # 88% Active, 7% Inactive, 3% Suspended, 2% Closed
    
    return random.choices(statuses, weights=weights)[0]

# ====================================
# "is_online_payment_enabled" data - ADDED MISSING FIELD
# ====================================
def generate_is_online_payment_enabled(account_type, account_status):
    """
    Generate online payment enabled flag
    Schema: is_online_payment_enabled BOOLEAN DEFAULT TRUE
    
    Args:
        account_type (str): Account type
        account_status (str): Account status
        
    Returns:
        bool: True if online payment is enabled
    """
    
    # Inactive/Suspended/Closed accounts don't have online payment
    if account_status in ['Inactive', 'Suspended', 'Closed']:
        return False
    
    # Fixed deposits typically don't have online payment
    if account_type == 'Fixed_Deposit':
        return random.random() < 0.1  # 10% chance
    
    # Loan accounts sometimes don't have online payment
    if account_type == 'Loan':
        return random.random() < 0.6  # 60% chance
    
    # Savings and Current accounts almost always have online payment
    return random.random() < 0.95  # 95% chance

# ====================================
# "interest_rate" data - ADDED MISSING FIELD
# ====================================
def generate_interest_rate(account_type):
    """
    Generate interest rate based on account type
    Schema: interest_rate DECIMAL(5,4) DEFAULT 0.0000
    
    Args:
        account_type (str): Account type
        
    Returns:
        float: Annual interest rate (e.g., 0.0350 = 3.5%)
    """
    
    if account_type == 'Savings':
        # Savings accounts: 1.5% - 4.5% annual
        return round(random.uniform(0.015, 0.045), 4)
    
    elif account_type == 'Fixed_Deposit':
        # Fixed deposits: 4.0% - 8.0% annual
        return round(random.uniform(0.040, 0.080), 4)
    
    elif account_type == 'Current':
        # Current accounts: 0.1% - 1.0% annual
        return round(random.uniform(0.001, 0.010), 4)
    
    else:  # Loan
        # Loan accounts: 8.0% - 18.0% annual (lending rate)
        return round(random.uniform(0.080, 0.180), 4)

# ====================================
# "last_transaction_at" data - ADDED MISSING FIELD
# ====================================
def generate_last_transaction_at(account_creation_time, account_status):
    """
    Generate last transaction timestamp
    Schema: last_transaction_at TIMESTAMPTZ (can be NULL)
    
    Args:
        account_creation_time (datetime): When account was created
        account_status (str): Account status
        
    Returns:
        datetime or None: Last transaction time
    """
    from datetime import datetime, timedelta
    
    # Closed accounts might not have recent transactions
    if account_status == 'Closed':
        if random.random() < 0.3:  # 30% chance of no transactions
            return None
        # Last transaction was before closure (1-30 days ago)
        days_ago = random.randint(1, 30)
        return datetime.now() - timedelta(days=days_ago)
    
    # Inactive accounts less likely to have recent transactions
    if account_status == 'Inactive':
        if random.random() < 0.5:  # 50% chance of no recent transactions
            return None
        # Last transaction 7-90 days ago
        days_ago = random.randint(7, 90)
        return datetime.now() - timedelta(days=days_ago)
    
    # Active accounts usually have recent transactions
    if account_status == 'Active':
        # 90% chance of transactions within last 30 days
        if random.random() < 0.9:
            days_ago = random.randint(0, 30)
            hours_ago = random.randint(0, 23)
            return datetime.now() - timedelta(days=days_ago, hours=hours_ago)
    else:
            return None
    
    # Suspended accounts
    if random.random() < 0.7:  # 70% chance of no recent transactions
        return None
    days_ago = random.randint(1, 60)
    return datetime.now() - timedelta(days=days_ago) 