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
def generate_currency(customer_type='Individual'):
    """
    Generate currency with realistic distribution
    
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

# ============================
# "daily_transfer_limit" data
# ============================
def generate_daily_transfer_limit(account_type, customer_type='Individual'):
    """
    Generate realistic daily transfer limit based on account type and customer type
    
    Args:
        account_type (str): Account type
        customer_type (str): 'Individual' or 'Organization'
        
    Returns:
        int: Daily transfer limit in VND
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
    
    return limit

# ==================================
# "daily_online_payment_limit" data
# ==================================
def generate_daily_online_payment_limit(daily_transfer_limit):
    """
    Generate daily online payment limit (typically lower than transfer limit)
    
    Args:
        daily_transfer_limit (int): Daily transfer limit
        
    Returns:
        int: Daily online payment limit in VND
    """
    
    # Online payment limit is typically 40-70% of transfer limit
    percentage = random.uniform(0.4, 0.7)
    payment_limit = int(daily_transfer_limit * percentage)
    
    # Round to nearest 100K for realism
    payment_limit = round(payment_limit / 100_000) * 100_000
    
    # Minimum 5M VND
    payment_limit = max(payment_limit, 5_000_000)
    
    return payment_limit

# ======================
# "account_status" data
# ======================
def generate_account_status():
    """
    Generate account status with realistic distribution
    
    Returns:
        str: Account status - 'Active', 'Inactive', 'Suspended', 'Closed'
    """
    
    # Most accounts are active
    statuses = ['Active', 'Inactive', 'Suspended', 'Closed']
    weights = [0.88, 0.07, 0.03, 0.02]  # 88% Active, 7% Inactive, 3% Suspended, 2% Closed
    
    return random.choices(statuses, weights=weights)[0]

# =====================
# "is_primary" data
# =====================
def generate_is_primary(existing_accounts_count=0):
    """
    Generate is_primary flag (only one primary account per customer)
    
    Args:
        existing_accounts_count (int): Number of existing accounts for this customer
        
    Returns:
        bool: True if this is primary account, False otherwise
    """
    
    if existing_accounts_count == 0:
        # First account is usually primary
        return random.random() < 0.9  # 90% chance first account is primary
    else:
        # Additional accounts are rarely primary
        return random.random() < 0.1  # 10% chance additional account is primary 