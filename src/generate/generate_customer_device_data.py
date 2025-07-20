import hashlib
import random
import uuid

# =====================
# "device_type" data
# =====================
# Track used device identifiers to ensure uniqueness
_used_device_identifiers = set()

def generate_device_type():
    """
    Generate device type with realistic distribution for Vietnamese banking
    
    Returns:
        str: Device type - 'Mobile', 'Desktop', 'Tablet'
    """
    
    # Based on Vietnamese banking app usage patterns
    device_types = ['Mobile', 'Desktop', 'Tablet']
    weights = [0.75, 0.20, 0.05]  # 75% Mobile, 20% Desktop, 5% Tablet
    
    return random.choices(device_types, weights=weights)[0]

# =========================
# "device_identifier" data
# =========================
def generate_device_identifier(device_type, customer_id):
    """
    Generate device identifier with format: {TYPE}:{IDENTIFIER}
    Must match regex: ^(IMEI|MAC|UUID|ANDROID_ID):[A-Za-z0-9:-]+$
    
    Args:
        device_type (str): 'Mobile', 'Desktop', 'Tablet'
        customer_id (str): Customer UUID for uniqueness
        
    Returns:
        str: Device identifier in format TYPE:IDENTIFIER
    """
    max_attempts = 100
    attempts = 0
    
    while attempts < max_attempts:
        # Choose identifier type based on device type
        if device_type == 'Mobile':
            # Mobile: 70% IMEI, 20% UUID (iOS), 10% ANDROID_ID
            identifier_type = random.choices(['IMEI', 'UUID', 'ANDROID_ID'], [0.7, 0.2, 0.1])[0]
        elif device_type == 'Desktop':
            # Desktop: 80% MAC, 20% UUID
            identifier_type = random.choices(['MAC', 'UUID'], [0.8, 0.2])[0]
        else:  # Tablet
            # Tablet: 60% MAC, 30% UUID, 10% ANDROID_ID
            identifier_type = random.choices(['MAC', 'UUID', 'ANDROID_ID'], [0.6, 0.3, 0.1])[0]
        
        # Generate identifier based on type
        if identifier_type == 'IMEI':
            # IMEI: 15 digits (International Mobile Equipment Identity)
            identifier = _generate_imei(customer_id, attempts)
        elif identifier_type == 'MAC':
            # MAC: 6 groups of 2 hex digits separated by colons
            identifier = _generate_mac_address(customer_id, attempts)
        elif identifier_type == 'UUID':
            # UUID: Standard UUID format
            identifier = _generate_uuid_identifier(customer_id, attempts)
        else:  # ANDROID_ID
            # ANDROID_ID: 16 hex characters
            identifier = _generate_android_id(customer_id, attempts)
        
        # Format as TYPE:IDENTIFIER
        device_identifier = f"{identifier_type}:{identifier}"
        
        # Check uniqueness
        if device_identifier not in _used_device_identifiers:
            _used_device_identifiers.add(device_identifier)
            return device_identifier
        
        attempts += 1
    
    raise Exception(f"Could not generate unique device identifier after {max_attempts} attempts")

# =====================
# "device_name" data
# =====================
def _generate_imei(customer_id, attempts):
    """Generate realistic IMEI (15 digits)"""
    # Create seed for deterministic generation
    seed_string = f"IMEI_{customer_id}_{attempts}"
    seed_value = int(hashlib.md5(seed_string.encode()).hexdigest()[:12], 16)
    
    # IMEI format: TAC (8 digits) + Serial (6 digits) + Check digit (1)
    # Use realistic TAC codes (Type Allocation Code)
    realistic_tacs = [
        '35328910',  # Apple iPhone
        '35404511',  # Samsung Galaxy
        '86781905',  # Oppo
        '35875510',  # Xiaomi
        '35171005',  # Vivo
    ]
    
    tac = random.choice(realistic_tacs)
    serial = f"{(seed_value % 1000000):06d}"
    
    # Simple check digit (not real Luhn algorithm for demo)
    check_digit = str(seed_value % 10)
    
    return f"{tac}{serial}{check_digit}"

# =====================
# "device_name" data
# =====================
def _generate_mac_address(customer_id, attempts):
    """Generate realistic MAC address"""
    seed_string = f"MAC_{customer_id}_{attempts}"
    seed_value = int(hashlib.md5(seed_string.encode()).hexdigest()[:12], 16)
    
    # Generate 6 bytes for MAC address
    mac_bytes = []
    temp_seed = seed_value
    for _ in range(6):
        mac_bytes.append(f"{(temp_seed % 256):02X}")
        temp_seed //= 256
    
    return ":".join(mac_bytes)

# =====================
# "device_name" data
# =====================
def _generate_uuid_identifier(customer_id, attempts):
    """Generate UUID for device identification"""
    seed_string = f"UUID_{customer_id}_{attempts}"
    # Create deterministic UUID based on seed
    seed_bytes = hashlib.md5(seed_string.encode()).digest()
    
    # Format as UUID string
    return str(uuid.UUID(bytes=seed_bytes))


def _generate_android_id(customer_id, attempts):
    """Generate Android ID (16 hex characters)"""
    seed_string = f"ANDROID_{customer_id}_{attempts}"
    seed_value = int(hashlib.md5(seed_string.encode()).hexdigest()[:16], 16)
    
    return f"{seed_value:016x}"


def generate_device_name(device_type):
    """
    Generate realistic device name based on Vietnamese market
    
    Args:
        device_type (str): 'Mobile', 'Desktop', 'Tablet'
        
    Returns:
        str or None: Device name (70% probability) or None (30%)
    """
    
    # 30% chance of no device name
    if random.random() < 0.3:
        return None
    
    if device_type == 'Mobile':
        # Popular mobile devices in Vietnam
        mobile_names = [
            'iPhone 15', 'iPhone 15 Pro', 'iPhone 14', 'iPhone 13',
            'Samsung Galaxy S24', 'Samsung Galaxy S23', 'Samsung Galaxy A54',
            'Samsung Galaxy A34', 'Samsung Galaxy Note 20',
            'Oppo Reno11', 'Oppo Find X6', 'Oppo A98', 'Oppo A78',
            'Xiaomi 14', 'Xiaomi 13', 'Xiaomi Redmi Note 13', 'Xiaomi Redmi 12',
            'Vivo V30', 'Vivo Y36', 'Vivo X100',
            'Realme 11', 'Realme C55',
            'Huawei P60', 'Huawei Nova 11'
        ]
        return random.choice(mobile_names)
    
    elif device_type == 'Desktop':
        # Common desktop/laptop names
        desktop_names = [
            'Windows PC', 'Dell Desktop', 'HP Desktop', 'Asus Desktop',
            'MacBook Pro', 'MacBook Air', 'iMac',
            'Dell Laptop', 'HP Laptop', 'Asus Laptop', 'Lenovo Laptop',
            'Acer Laptop', 'MSI Laptop', 'ThinkPad',
            'Surface Laptop', 'Surface Pro'
        ]
        return random.choice(desktop_names)
    
    else:  # Tablet
        # Popular tablets
        tablet_names = [
            'iPad Air', 'iPad Pro', 'iPad mini', 'iPad',
            'Samsung Galaxy Tab S9', 'Samsung Galaxy Tab A9', 'Samsung Galaxy Tab S8',
            'Lenovo Tab P12', 'Lenovo Tab M10', 'Lenovo Tab P11',
            'Huawei MatePad', 'Huawei MatePad Pro',
            'Xiaomi Pad 6', 'Xiaomi Pad 5'
        ]
        return random.choice(tablet_names)

# =====================
# "is_trusted" data
# =====================
def generate_is_trusted(device_count_for_customer=1, device_type='Mobile'):
    """
    Generate is_trusted flag based on device usage patterns
    
    Args:
        device_count_for_customer (int): How many devices this customer has
        device_type (str): Device type
        
    Returns:
        bool: True if device is trusted
    """
    
    # Base trust probability
    if device_count_for_customer == 1:
        # First device - usually primary and trusted
        base_trust = 0.8
    else:
        # Additional devices - lower trust initially
        base_trust = 0.3
    
    # Device type modifier
    if device_type == 'Desktop':
        # Desktop devices typically more trusted (work/home computers)
        base_trust *= 1.2
    elif device_type == 'Mobile':
        # Mobile devices standard trust
        base_trust *= 1.0
    else:  # Tablet
        # Tablets slightly less trusted (shared devices)
        base_trust *= 0.9
    
    # Cap at 1.0
    trust_probability = min(base_trust, 1.0)
    
    return random.random() < trust_probability

# =====================
# "device_status" data
# =====================
def generate_device_status():
    """
    Generate device status with realistic distribution
    
    Returns:
        str: Device status - 'Active', 'Blocked', 'Expired'
    """
    
    # Most devices are active
    statuses = ['Active', 'Blocked', 'Expired']
    weights = [0.85, 0.10, 0.05]  # 85% Active, 10% Blocked, 5% Expired
    
    return random.choices(statuses, weights=weights)[0]


def reset_device_identifier_tracking():
    """
    Reset device identifier tracking (useful for testing or new generation sessions)
    """
    global _used_device_identifiers
    _used_device_identifiers.clear() 