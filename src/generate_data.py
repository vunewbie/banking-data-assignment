from datetime import datetime, date, timedelta
import random

def get_daily_seed():
    """
    Generate a daily seed number based on current date
    Calculate sum of all digits in DDMMYYYY format then multiply by 100
    
    Example: 18/07/2025 -> (1+8+0+7+2+0+2+5) * 100 = 2500
    
    Returns:
        int: Date-based seed number for reproducible random generation
    """
    today = datetime.now()
    date_string = today.strftime("%d%m%Y")  # Format: DDMMYYYY
    
    # Sum all digits in the date string
    digit_sum = sum(int(digit) for digit in date_string)
    
    # Multiply by 100
    seed = digit_sum * 100
    return seed

# =====================================================
# Customer data generation
# =====================================================
# =====================================================
# "full_name" data
# =====================================================
VIETNAMESE_NAMES = {
    'surnames': [
        'Nguyễn', 'Trần', 'Lê', 'Phạm', 'Huỳnh', 'Hoàng', 'Võ', 'Vũ', 
        'Phan', 'Trương', 'Bùi', 'Đặng', 'Đỗ', 'Ngô', 'Hồ', 'Dương', 
        'Đinh', 'Đoàn', 'Lâm', 'Mai', 'Trịnh', 'Đào'
    ],
    'middle_names': [
        'Văn', 'Thị', 'Hoàng', 'Minh', 'Quang', 'Thanh', 'Hữu', 'Thành',
        'Công', 'Đức', 'Bá', 'Xuân', 'Thu', 'Hạ', 'Đông', 'Kim', 
        'Ngọc', 'Phước', 'Tuấn', 'Anh', 'Hồng', 'Lan'
    ],
    'given_names': [
        'An', 'Bình', 'Châu', 'Dũng', 'Hà', 'Hưng', 'Khang', 'Linh',
        'Mai', 'Nam', 'Oanh', 'Phong', 'Quân', 'Sơn', 'Thảo', 'Tùng',
        'Uyên', 'Vinh', 'Yến', 'Đạt', 'Hải', 'Khánh', 'Long', 'Trang',
        'Hoa', 'Hùng', 'Kiên', 'Nhung', 'Quỳnh', 'Thắng', 'Vân', 'Xuân'
    ]
}

def generate_full_name():
    """
    Generate a random Vietnamese full name
    
    Returns:
        str: Full name in format "Surname Middle_name Given_name"
    """
    surname = random.choice(VIETNAMESE_NAMES['surnames'])
    middle_name = random.choice(VIETNAMESE_NAMES['middle_names'])
    given_name = random.choice(VIETNAMESE_NAMES['given_names'])
    
    full_name = f"{surname} {middle_name} {given_name}"
    return full_name

# =====================================================
# "gender" data
# =====================================================
GENDER_INDICATORS = {
    'female_middle_names': ['Thị', 'Kim', 'Ngọc', 'Hồng', 'Lan', 'Thu', 'Hạ'],
    'male_middle_names': ['Văn', 'Công', 'Đức', 'Bá', 'Hữu', 'Tuấn'],
    'neutral_middle_names': ['Hoàng', 'Minh', 'Quang', 'Thanh', 'Thành', 'Xuân', 'Đông', 'Anh', 'Phước'],
    'female_given_names': ['Mai', 'Linh', 'Oanh', 'Thảo', 'Uyên', 'Yến', 'Trang', 'Hoa', 'Nhung', 'Quỳnh', 'Vân'],
    'male_given_names': ['An', 'Bình', 'Dũng', 'Hưng', 'Khang', 'Nam', 'Phong', 'Quân', 'Sơn', 'Tùng', 'Vinh', 'Đạt', 'Hải', 'Khánh', 'Long', 'Hùng', 'Kiên', 'Thắng'],
    'neutral_given_names': ['Châu', 'Hà', 'Xuân']
}

def generate_gender(full_name):
    """
    Generate gender based on Vietnamese name patterns
    
    Args:
        full_name (str): Full name in "Surname Middle_name Given_name" format
        
    Returns:
        str: "Male" or "Female"
    """
    name_parts = full_name.split()
    middle_name = name_parts[1] if len(name_parts) >= 2 else ""
    given_name = name_parts[2] if len(name_parts) >= 3 else ""
    
    # Check middle name first (strongest indicator)
    if middle_name in GENDER_INDICATORS['female_middle_names']:
        return "Female"
    elif middle_name in GENDER_INDICATORS['male_middle_names']:
        return "Male"
    
    # Check given name if middle name is neutral
    elif given_name in GENDER_INDICATORS['female_given_names']:
        return "Female"
    elif given_name in GENDER_INDICATORS['male_given_names']:
        return "Male"
    
    # Default to random if can't determine (52% Male, 48% Female - VN ratio)
    return "Male" if random.random() < 0.52 else "Female"

# =====================================================
# "date_of_birth" data
# =====================================================
def generate_date_of_birth():
    """
    Generate realistic date of birth for banking customers
    Age distribution: 18-70 years old, peak at 25-45 years
    
    Returns:
        date: Date of birth as Python date object
    """
    today = date.today()
    
    # Age distribution weights (realistic banking customers)
    age_ranges = [
        (18, 24, 0.15),   # Young adults: 15%
        (25, 35, 0.35),   # Prime banking age: 35%
        (36, 45, 0.30),   # Established customers: 30%
        (46, 55, 0.15),   # Middle-aged: 15%
        (56, 70, 0.05),   # Senior customers: 5%
    ]
    
    # Select age range based on weights
    ranges, weights = zip(*[(r[:2], r[2]) for r in age_ranges])
    selected_range = random.choices(ranges, weights=weights)[0]
    
    # Generate random age within selected range
    age = random.randint(selected_range[0], selected_range[1])
    
    # Calculate birth year
    birth_year = today.year - age
    
    # Generate random month and day
    birth_month = random.randint(1, 12)
    
    # Handle different days in months
    days_in_month = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    
    # Check for leap year
    if birth_month == 2 and birth_year % 4 == 0 and (birth_year % 100 != 0 or birth_year % 400 == 0):
        max_day = 29
    else:
        max_day = days_in_month[birth_month - 1]
    
    birth_day = random.randint(1, max_day)
    
    return date(birth_year, birth_month, birth_day)

# =====================================================
# "phone_number" data
# =====================================================
# Vietnamese mobile prefixes (official VNPT, Viettel, MobiFone, Vietnamobile, Gmobile)
# 2-digit prefixes (without leading 0)
VIETNAMESE_PHONE_PREFIXES = [
    # Viettel
    '86', '96', '97', '98', '32', '33', '34', '35', '36', '37', '38', '39',
    # VNPT (VinaPhone)
    '88', '91', '94', '83', '84', '85', '81', '82',
    # MobiFone  
    '89', '90', '93', '70', '79', '77', '76', '78',
    # Vietnamobile
    '92', '56', '58',
    # Gmobile
    '99', '59'
]

# Track used phone numbers to ensure uniqueness
_used_phone_numbers = set()

def generate_phone_number():
    """
    Generate a Vietnamese mobile phone number (90% valid format, 10% invalid for testing)
    Valid format: 0xxxxxxxxx (total 10 digits: 0 + 2-digit prefix + 7-digit suffix)
    Invalid format: Wrong suffix length for data quality testing
    
    Returns:
        str: Vietnamese phone number in format 0xxxxxxxxx
    """
    max_attempts = 1000  # Prevent infinite loop
    attempts = 0
    
    while attempts < max_attempts:
        # Select random Vietnamese mobile prefix
        prefix = random.choice(VIETNAMESE_PHONE_PREFIXES)
        
        # Generate suffix: 90% correct (7 digits), 10% incorrect (different length)
        if random.random() < 0.9:
            # 90% - Correct format: exactly 7 digits
            suffix = ''.join([str(random.randint(0, 9)) for _ in range(7)])
        else:
            # 10% - Incorrect format: wrong length for data quality testing
            wrong_lengths = [5, 6, 8, 9]  # Various wrong lengths
            wrong_length = random.choice(wrong_lengths)
            suffix = ''.join([str(random.randint(0, 9)) for _ in range(wrong_length)])
        
        # Vietnamese phone format: 0 + prefix + suffix
        phone_number = f"0{prefix}{suffix}"
        
        # Check uniqueness
        if phone_number not in _used_phone_numbers:
            _used_phone_numbers.add(phone_number)
            return phone_number
        
        attempts += 1
    
    # Fallback if somehow we can't generate unique number
    raise Exception(f"Could not generate unique phone number after {max_attempts} attempts")


def reset_phone_tracking():
    """
    Reset phone number tracking (useful for testing or new generation sessions)
    """
    global _used_phone_numbers
    _used_phone_numbers.clear()


# =====================================================
# "email" data
# =====================================================
# Vietnamese diacritics mapping for email normalization
VIETNAMESE_DIACRITICS = {
    'à': 'a', 'á': 'a', 'ạ': 'a', 'ả': 'a', 'ã': 'a', 'â': 'a', 'ầ': 'a', 'ấ': 'a', 'ậ': 'a', 'ẩ': 'a', 'ẫ': 'a',
    'ă': 'a', 'ằ': 'a', 'ắ': 'a', 'ặ': 'a', 'ẳ': 'a', 'ẵ': 'a',
    'è': 'e', 'é': 'e', 'ẹ': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ê': 'e', 'ề': 'e', 'ế': 'e', 'ệ': 'e', 'ể': 'e', 'ễ': 'e',
    'ì': 'i', 'í': 'i', 'ị': 'i', 'ỉ': 'i', 'ĩ': 'i',
    'ò': 'o', 'ó': 'o', 'ọ': 'o', 'ỏ': 'o', 'õ': 'o', 'ô': 'o', 'ồ': 'o', 'ố': 'o', 'ộ': 'o', 'ổ': 'o', 'ỗ': 'o',
    'ơ': 'o', 'ờ': 'o', 'ớ': 'o', 'ợ': 'o', 'ở': 'o', 'ỡ': 'o',
    'ù': 'u', 'ú': 'u', 'ụ': 'u', 'ủ': 'u', 'ũ': 'u', 'ư': 'u', 'ừ': 'u', 'ứ': 'u', 'ự': 'u', 'ử': 'u', 'ữ': 'u',
    'ỳ': 'y', 'ý': 'y', 'ỵ': 'y', 'ỷ': 'y', 'ỹ': 'y',
    'đ': 'd',
    # Uppercase versions
    'À': 'A', 'Á': 'A', 'Ạ': 'A', 'Ả': 'A', 'Ã': 'A', 'Â': 'A', 'Ầ': 'A', 'Ấ': 'A', 'Ậ': 'A', 'Ẩ': 'A', 'Ẫ': 'A',
    'Ă': 'A', 'Ằ': 'A', 'Ắ': 'A', 'Ặ': 'A', 'Ẳ': 'A', 'Ẵ': 'A',
    'È': 'E', 'É': 'E', 'Ẹ': 'E', 'Ẻ': 'E', 'Ẽ': 'E', 'Ê': 'E', 'Ề': 'E', 'Ế': 'E', 'Ệ': 'E', 'Ể': 'E', 'Ễ': 'E',
    'Ì': 'I', 'Í': 'I', 'Ị': 'I', 'Ỉ': 'I', 'Ĩ': 'I',
    'Ò': 'O', 'Ó': 'O', 'Ọ': 'O', 'Ỏ': 'O', 'Õ': 'O', 'Ô': 'O', 'Ồ': 'O', 'Ố': 'O', 'Ộ': 'O', 'Ổ': 'O', 'Ỗ': 'O',
    'Ơ': 'O', 'Ờ': 'O', 'Ớ': 'O', 'Ợ': 'O', 'Ở': 'O', 'Ỡ': 'O',
    'Ù': 'U', 'Ú': 'U', 'Ụ': 'U', 'Ủ': 'U', 'Ũ': 'U', 'Ư': 'U', 'Ừ': 'U', 'Ứ': 'U', 'Ự': 'U', 'Ử': 'U', 'Ữ': 'U',
    'Ỳ': 'Y', 'Ý': 'Y', 'Ỵ': 'Y', 'Ỷ': 'Y', 'Ỹ': 'Y',
    'Đ': 'D'
}

EMAIL_DOMAINS = [
    'gmail.com',      # 60% - Most popular
    'yahoo.com',      # 20% - Popular alternative  
    'outlook.com',    # 10% - Microsoft
    'email.com',      # 5% - Generic
    'hotmail.com'     # 5% - Legacy
]

def remove_vietnamese_diacritics(text):
    """
    Remove Vietnamese diacritics from text for email normalization
    
    Args:
        text (str): Text with Vietnamese diacritics
        
    Returns:
        str: Text with diacritics removed
    """
    result = ""
    for char in text:
        # Use get with fallback to ensure no None values
        normalized_char = VIETNAMESE_DIACRITICS.get(char) or char
        result += normalized_char
    return result

def generate_email(full_name, phone_number):
    """
    Generate email address based on name and phone number (30% have email, 70% null)
    Email format: {normalized_given_name}{phone_number}@{domain}
    
    Args:
        full_name (str): Full Vietnamese name
        phone_number (str): Phone number in format 0xxxxxxxxx for uniqueness
        
    Returns:
        str or None: Email address or None (70% chance)
    """
    # 70% null values (not everyone has email)
    if random.random() < 0.7:
        return None
    
    # Extract given name (last part)
    name_parts = full_name.split()
    given_name = name_parts[-1] if name_parts else "user"
    
    # Normalize Vietnamese name for email
    normalized_name = remove_vietnamese_diacritics(given_name.lower())
    
    # Use full phone number for uniqueness (keep entire 0xxxxxxxxx format)
    # No modification needed - use phone_number directly
    
    # Select random domain
    domain_weights = [0.6, 0.2, 0.1, 0.05, 0.05]
    domain = random.choices(EMAIL_DOMAINS, weights=domain_weights)[0]
    
    # Generate email
    email = f"{normalized_name}{phone_number}@{domain}"
    
    return email


if __name__ == "__main__":
    # Test the functions
    seed = get_daily_seed()
    print(f"Today's seed: {seed}")
    
    # Set seed for reproducible testing
    random.seed(seed)
    
    # Reset phone tracking for clean test
    reset_phone_tracking()
    
    # Test customer data generation
    print("\nSample customer data:")
    valid_phones = 0
    invalid_phones = 0
    email_count = 0
    null_email_count = 0
    
    for i in range(20):  # Test with more samples to see variations
        name = generate_full_name()
        gender = generate_gender(name)
        dob = generate_date_of_birth()
        phone = generate_phone_number()
        email = generate_email(name, phone)
        
        # Format date for Vietnamese display (dd/mm/yyyy)
        dob_formatted = dob.strftime("%d/%m/%Y")
        age = date.today().year - dob.year
        
        # Check phone format for testing
        expected_length = 10  # 0 (1) + prefix (3) + suffix (6) = 10
        is_valid_phone = len(phone) == expected_length
        if is_valid_phone:
            valid_phones += 1
        else:
            invalid_phones += 1
        
        # Track email statistics
        if email is not None:
            email_count += 1
            email_display = email
        else:
            null_email_count += 1
            email_display = "NULL"
            
        phone_status = "✅" if is_valid_phone else f"❌ (len: {len(phone)})"
        print(f"{i+1}. {name} - {gender} - {dob_formatted} ({age} tuổi)")
        print(f"    📱 {phone} {phone_status}")
        print(f"    📧 {email_display}")
        print()
    
    print(f"Generation summary:")
    print(f"📱 Phone numbers:")
    print(f"  - Total generated: {len(_used_phone_numbers)}")
    print(f"  - Valid format: {valid_phones} ({valid_phones/20*100:.1f}%)")
    print(f"  - Invalid format: {invalid_phones} ({invalid_phones/20*100:.1f}%)")
    print(f"  - Expected: ~90% valid, ~10% invalid")
    print(f"📧 Email addresses:")
    print(f"  - With email: {email_count} ({email_count/20*100:.1f}%)")
    print(f"  - NULL email: {null_email_count} ({null_email_count/20*100:.1f}%)")
    print(f"  - Expected: ~30% with email, ~70% NULL")
