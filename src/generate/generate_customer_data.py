from datetime import date, timedelta
import random
import hashlib
import secrets

# =====================================================
# "full_name" data
# =====================================================
NAMES = {
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
    Generate a random full name
    
    Returns:
        str: Full name in format "Surname Middle_name Given_name"
    """
    surname = random.choice(NAMES['surnames'])
    middle_name = random.choice(NAMES['middle_names'])
    given_name = random.choice(NAMES['given_names'])
    
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
    Generate gender based on name patterns
    
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
    Generate realistic date of birth for customers
    Age distribution: 18-70, peak at 25-45
    
    Returns:
        date: Date of birth as Python date object
    """
    today = date.today()
    
    # Age distribution weights
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
# Mobile prefixes without leading 0 (official VNPT, Viettel, MobiFone, Vietnamobile, Gmobile)
PHONE_PREFIXES = [
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
    Generate a mobile phone number (90% valid format, 10% invalid for testing)
    Valid format: 0xxxxxxxxx (total 10 digits: 0 + 2-digit prefix + 7-digit suffix)
    Invalid format: Wrong suffix length for data quality testing
    
    Returns:
        str: Vietnamese phone number in format 0xxxxxxxxx
    """
    max_attempts = 1000  # Prevent infinite loop
    attempts = 0
    
    while attempts < max_attempts:
        # Select random mobile prefix
        prefix = random.choice(PHONE_PREFIXES)
        
        # Generate suffix: 90% correct (7 digits), 10% incorrect (different length)
        if random.random() < 0.9:
            # 90% - Correct format: exactly 7 digits
            suffix = ''.join([str(random.randint(0, 9)) for _ in range(7)])
        else:
            # 10% - Incorrect format: wrong length for data quality testing
            wrong_lengths = [5, 6, 8, 9]  # Various wrong lengths
            wrong_length = random.choice(wrong_lengths)
            suffix = ''.join([str(random.randint(0, 9)) for _ in range(wrong_length)])
        
        # Phone format: 0 + prefix + suffix
        phone_number = f"0{prefix}{suffix}"
        
        # Check uniqueness
        if phone_number not in _used_phone_numbers:
            _used_phone_numbers.add(phone_number)
            return phone_number
        
        attempts += 1
    
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

    # Select random domain
    domain_weights = [0.6, 0.2, 0.1, 0.05, 0.05]
    domain = random.choices(EMAIL_DOMAINS, weights=domain_weights)[0]
    
    # Generate email
    email = f"{normalized_name}{phone_number}@{domain}"
    
    return email

# =====================================================
# "tax_identification_number" data
# =====================================================
def generate_tax_identification_number():
    """
    Generate Vietnamese tax identification number (90% valid format, 10% invalid for testing)
    Valid formats:
    - Personal tax ID: 10 digits (for individual taxpayers)
    - Business tax ID: 13 digits (for enterprises)
    Invalid format: Wrong number length for data quality testing
    
    Returns:
        str: Tax identification number
    """
    if random.random() < 0.9:
        # 90% - Valid format
        if random.random() < 0.9:
            # 90% of valid ones are personal tax ID (10 digits)
            tax_id = ''.join([str(random.randint(0, 9)) for _ in range(10)])
        else:
            # 10% of valid ones are business tax ID (13 digits)
            tax_id = ''.join([str(random.randint(0, 9)) for _ in range(13)])
    else:
        # 10% - Invalid format (wrong length for data quality testing)
        wrong_lengths = [8, 9, 11, 12, 14, 15]
        wrong_length = random.choice(wrong_lengths)
        tax_id = ''.join([str(random.randint(0, 9)) for _ in range(wrong_length)])
    
    return tax_id

# =====================================================================================
# "id_passport_number", "issue_date", "expiry_date", "issuing_authority" data
# =====================================================================================
# Vietnamese issuing authorities for CCCD/Passport
ISSUING_AUTHORITIES = {
    'cccd': [
        'Cục Cảnh sát quản lý hành chính về trật tự xã hội',
        'Công an TP. Hồ Chí Minh', 
        'Công an TP. Hà Nội',
        'Công an tỉnh An Giang',
        'Công an tỉnh Bà Rịa - Vũng Tàu',
        'Công an tỉnh Bạc Liêu',
        'Công an tỉnh Bắc Giang',
        'Công an tỉnh Bắc Kạn',
        'Công an tỉnh Bắc Ninh',
        'Công an tỉnh Bến Tre',
        'Công an tỉnh Bình Định',
        'Công an tỉnh Bình Dương',
        'Công an tỉnh Bình Phước',
        'Công an tỉnh Bình Thuận',
        'Công an tỉnh Cà Mau',
        'Công an tỉnh Cao Bằng',
        'Công an tỉnh Đắk Lắk',
        'Công an tỉnh Đắk Nông',
        'Công an tỉnh Điện Biên',
        'Công an tỉnh Đồng Nai',
        'Công an tỉnh Đồng Tháp',
        'Công an tỉnh Gia Lai',
        'Công an tỉnh Hà Giang',
        'Công an tỉnh Hà Nam',
        'Công an tỉnh Hà Tĩnh',
        'Công an tỉnh Hải Dương',
        'Công an tỉnh Hậu Giang',
        'Công an tỉnh Hòa Bình',
        'Công an tỉnh Hưng Yên',
        'Công an tỉnh Khánh Hòa',
        'Công an tỉnh Kiên Giang',
        'Công an tỉnh Kon Tum',
        'Công an tỉnh Lai Châu',
        'Công an tỉnh Lâm Đồng',
        'Công an tỉnh Lạng Sơn',
        'Công an tỉnh Lào Cai',
        'Công an tỉnh Long An',
        'Công an tỉnh Nam Định',
        'Công an tỉnh Nghệ An',
        'Công an tỉnh Ninh Bình',
        'Công an tỉnh Ninh Thuận',
        'Công an tỉnh Phú Thọ',
        'Công an tỉnh Phú Yên',
        'Công an tỉnh Quảng Bình',
        'Công an tỉnh Quảng Nam',
        'Công an tỉnh Quảng Ngãi',
        'Công an tỉnh Quảng Ninh',
        'Công an tỉnh Quảng Trị',
        'Công an tỉnh Sóc Trăng',
        'Công an tỉnh Sơn La',
        'Công an tỉnh Tây Ninh',
        'Công an tỉnh Thái Bình',
        'Công an tỉnh Thái Nguyên',
        'Công an tỉnh Thanh Hóa',
        'Công an tỉnh Thừa Thiên Huế',
        'Công an tỉnh Tiền Giang',
        'Công an tỉnh Trà Vinh',
        'Công an tỉnh Tuyên Quang',
        'Công an tỉnh Vĩnh Long',
        'Công an tỉnh Vĩnh Phúc',
        'Công an tỉnh Yên Bái'
    ],
    'passport': [
        'Cục Quản lý xuất nhập cảnh - Bộ Công an',
        'Chi cục Quản lý xuất nhập cảnh - Công an TP. Hồ Chí Minh',
        'Chi cục Quản lý xuất nhập cảnh - Công an TP. Hà Nội',
        'Chi cục Quản lý xuất nhập cảnh - Công an TP. Đà Nẵng',
        'Chi cục Quản lý xuất nhập cảnh - Công an TP. Cần Thơ'
    ]
}

def generate_id_passport_number():
    """
    Generate Vietnamese CCCD or Passport number (90% valid format, 10% invalid for testing)
    Valid formats:
    - CCCD: 12 digits (most common for Vietnamese citizens)
    - Passport: 1 letter + 7 digits (for international travel)
    Invalid format: Wrong format/length for data quality testing
    
    Returns:
        tuple: (id_number, document_type) where document_type is 'CCCD' or 'Passport'
    """
    if random.random() < 0.9:
        # 90% - Valid format
        if random.random() < 0.8:
            # 80% are CCCD (most Vietnamese citizens have CCCD)
            id_number = ''.join([str(random.randint(0, 9)) for _ in range(12)])
            doc_type = 'CCCD'
        else:
            # 20% are Passport
            letter = random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
            digits = ''.join([str(random.randint(0, 9)) for _ in range(7)])
            id_number = f"{letter}{digits}"
            doc_type = 'Passport'
    else:
        # 10% - Invalid format for data quality testing
        if random.random() < 0.5:
            # Invalid CCCD (wrong length)
            wrong_lengths = [10, 11, 13, 14]
            wrong_length = random.choice(wrong_lengths)
            id_number = ''.join([str(random.randint(0, 9)) for _ in range(wrong_length)])
            doc_type = 'CCCD'
        else:
            # Invalid Passport (wrong format)
            if random.random() < 0.5:
                # Too many letters
                letters = ''.join([random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in range(2)])
                digits = ''.join([str(random.randint(0, 9)) for _ in range(6)])
                id_number = f"{letters}{digits}"
            else:
                # Wrong digit count
                letter = random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
                wrong_digit_counts = [5, 6, 8, 9]
                digit_count = random.choice(wrong_digit_counts)
                digits = ''.join([str(random.randint(0, 9)) for _ in range(digit_count)])
                id_number = f"{letter}{digits}"
            doc_type = 'Passport'
    
    return id_number, doc_type

def generate_issue_date(date_of_birth):
    """
    Generate issue date for CCCD/Passport (must be after 18th birthday)
    
    Args:
        date_of_birth (datetime.date): Birth date of the person
        
    Returns:
        datetime.date: Issue date
    """
    # CCCD/Passport is issued after 15th birthday
    # Handle leap year edge case (Feb 29)
    try:
        min_issue_date = date_of_birth.replace(year=date_of_birth.year + 15)
    except ValueError:
        # If date_of_birth is Feb 29 and target year is not leap year, use Feb 28
        min_issue_date = date_of_birth.replace(year=date_of_birth.year + 15, day=28)
    
    # Don't issue in the future
    max_issue_date = date.today()
    
    # If person is not yet 15, set min_issue_date to today (edge case)
    if min_issue_date > max_issue_date:
        min_issue_date = max_issue_date
    
    # Random date between min and max
    days_range = (max_issue_date - min_issue_date).days
    if days_range <= 0:
        return max_issue_date
        
    random_days = random.randint(0, days_range)
    issue_date = min_issue_date + timedelta(days=random_days)
    
    return issue_date

def generate_expiry_date(issue_date, document_type, date_of_birth):
    """
    Generate expiry date based on issue date, document type and date of birth
    Following Vietnamese law requirements
    
    Args:
        issue_date (datetime.date): Date when document was issued
        document_type (str): 'CCCD' or 'Passport'
        date_of_birth (datetime.date): Customer's date of birth
        
    Returns:
        datetime.date or None: Expiry date (None for CCCD 60+ years old - unlimited validity)
    """
    # Calculate age at issue date
    age_at_issue = issue_date.year - date_of_birth.year
    if issue_date < date_of_birth.replace(year=issue_date.year):
        age_at_issue -= 1
    
    if document_type == 'CCCD':
        # CCCD must be renewed at ages 25, 40, 60 (Vietnamese Citizen ID Law 2023)
        if age_at_issue < 25:
            years_to_add = 25 - age_at_issue
        elif age_at_issue < 40:
            years_to_add = 40 - age_at_issue
        elif age_at_issue < 60:
            years_to_add = 60 - age_at_issue
        else:
            # 60+ years old: unlimited validity
            return None
    else:  # Passport
        # Vietnamese passport validity (Law 23/2023/QH15)
        if age_at_issue < 14:
            years_to_add = 5  # Under 14: 5 years validity
        else:
            years_to_add = 10  # 14+ years: 10 years validity
    
    # Handle leap year edge case (Feb 29)
    try:
        expiry_date = issue_date.replace(year=issue_date.year + years_to_add)
    except ValueError:
        # If issue_date is Feb 29 and target year is not leap year, use Feb 28
        expiry_date = issue_date.replace(year=issue_date.year + years_to_add, day=28)
    
    return expiry_date

def generate_issuing_authority(document_type):
    """
    Generate issuing authority based on document type
    
    Args:
        document_type (str): 'CCCD' or 'Passport'
        
    Returns:
        str: Issuing authority name
    """
    if document_type == 'CCCD':
        return random.choice(ISSUING_AUTHORITIES['cccd'])
    else:  # Passport
        return random.choice(ISSUING_AUTHORITIES['passport'])

# =====================================================================================
# "is_resident" data
# =====================================================================================
def generate_is_resident(document_type):
    """
    Generate resident status based on document type
    
    Business logic:
    - CCCD holders: Vietnamese citizens residing in Vietnam → TRUE
    - Passport holders: Foreigners or overseas Vietnamese → FALSE (mostly)
    
    Args:
        document_type (str): 'CCCD' or 'Passport'
        
    Returns:
        bool: True if resident, False if non-resident
    """
    if document_type == 'CCCD':
        # CCCD holders are always Vietnamese residents
        return True
    else:  # Passport
        # Passport holders are mostly non-residents (foreigners or Viet Kieu)
        # But 10% could be Vietnamese residents who have passport for travel
        return random.random() < 0.1

# =====================================================================================
# "occupation", "position" data
# =====================================================================================
# Vietnamese occupations with realistic distribution
VIETNAMESE_OCCUPATIONS = {
    'Công chức/Viên chức': [
        'Công chức nhà nước', 'Viên chức y tế', 'Viên chức giáo dục', 
        'Công chức thuế', 'Công chức hải quan', 'Công chức công an'
    ],
    'Giáo dục': [
        'Giáo viên tiểu học', 'Giáo viên THCS', 'Giáo viên THPT',
        'Giảng viên đại học', 'Giáo viên mầm non', 'Huấn luyện viên'
    ],
    'Y tế': [
        'Bác sĩ', 'Y tá', 'Dược sĩ', 'Kỹ thuật viên y tế', 
        'Điều dưỡng', 'Bác sĩ răng hàm mặt'
    ],
    'Kỹ thuật': [
        'Kỹ sư phần mềm', 'Kỹ sư xây dựng', 'Kỹ sư cơ khí', 
        'Kỹ sư điện', 'Kỹ sư IT', 'Kỹ sư hóa học'
    ],
    'Kinh doanh/Tài chính': [
        'Nhân viên ngân hàng', 'Kế toán', 'Nhân viên bán hàng',
        'Chuyên viên tài chính', 'Nhân viên marketing', 'Chuyên viên đầu tư'
    ],
    'Dịch vụ': [
        'Nhân viên khách sạn', 'Nhân viên nhà hàng', 'Tài xế',
        'Bảo vệ', 'Thợ làm tóc', 'Masseur'
    ],
    'Sản xuất': [
        'Công nhân nhà máy', 'Thợ điện', 'Thợ máy', 
        'Công nhân xây dựng', 'Thợ hàn', 'Công nhân dệt may'
    ],
    'Nông nghiệp': [
        'Nông dân', 'Ngư dân', 'Chăn nuôi', 
        'Kỹ thuật viên nông nghiệp', 'Lâm nghiệp'
    ],
    'Tự do/Freelance': [
        'Freelancer IT', 'Photographer', 'Nhà thiết kế', 
        'Blogger/Youtuber', 'Dịch thuật viên', 'Gia sư'
    ],
    'Khác': [
        'Sinh viên', 'Hưu trí', 'Nội trợ', 'Thất nghiệp', 'Khởi nghiệp'
    ]
}

# Position hierarchy based on occupation and age
POSITION_LEVELS = {
    'entry': ['Thực tập sinh', 'Nhân viên', 'Công nhân', 'Tân binh'],
    'junior': ['Nhân viên', 'Chuyên viên', 'Kỹ thuật viên', 'Công nhân có kinh nghiệm'],
    'mid': ['Chuyên viên chính', 'Team Leader', 'Trưởng ca', 'Kỹ sư trưởng'],
    'senior': ['Chuyên viên cao cấp', 'Quản lý', 'Trưởng phòng', 'Giám sát'],
    'management': ['Giám đốc', 'Phó giám đốc', 'Trưởng ban', 'Chủ tịch'],
    'special': ['Hưu trí', 'Sinh viên', 'Nội trợ', 'Tự do', 'Chủ doanh nghiệp']
}

def generate_occupation():
    """
    Generate Vietnamese occupation with realistic distribution
    
    Returns:
        str: Occupation name
    """
    # Weight distribution based on Vietnamese labor market
    occupation_weights = {
        'Nông nghiệp': 0.25,        
        'Sản xuất': 0.20,           
        'Dịch vụ': 0.15,            
        'Kinh doanh/Tài chính': 0.12,
        'Công chức/Viên chức': 0.10,
        'Kỹ thuật': 0.08,
        'Giáo dục': 0.05,
        'Y tế': 0.03,
        'Tự do/Freelance': 0.015,
        'Khác': 0.005              # Student, retired, etc.
    }
    
    # Select occupation category
    categories = list(occupation_weights.keys())
    weights = list(occupation_weights.values())
    selected_category = random.choices(categories, weights=weights)[0]
    
    # Select specific occupation from category
    occupation = random.choice(VIETNAMESE_OCCUPATIONS[selected_category])
    
    return occupation

def generate_position(occupation, age):
    """
    Generate position based on occupation and age
    
    Args:
        occupation (str): Person's occupation
        age (int): Person's age
        
    Returns:
        str: Position/title
    """
    # Special cases for certain occupations
    if any(keyword in occupation.lower() for keyword in ['sinh viên', 'student']):
        return 'Sinh viên'
    elif any(keyword in occupation.lower() for keyword in ['hưu trí', 'retired']):
        return 'Hưu trí'
    elif any(keyword in occupation.lower() for keyword in ['nội trợ', 'housewife']):
        return 'Nội trợ'
    elif 'nông dân' in occupation.lower():
        return random.choice(['Nông dân', 'Chủ trang trại', 'Hợp tác xã viên'])
    elif 'ngư dân' in occupation.lower():
        return random.choice(['Ngư dân', 'Thuyền trưởng', 'Chủ ghe bầu'])
    
    # Age-based position determination
    if age < 25:
        # Young professionals
        level = 'entry'
    elif age < 30:
        # Early career
        level = random.choices(['entry', 'junior'], weights=[0.3, 0.7])[0]
    elif age < 40:
        # Mid career
        level = random.choices(['junior', 'mid'], weights=[0.4, 0.6])[0]
    elif age < 50:
        # Senior career
        level = random.choices(['mid', 'senior'], weights=[0.5, 0.5])[0]
    elif age < 60:
        # Late career / management
        level = random.choices(['senior', 'management'], weights=[0.6, 0.4])[0]
    else:
        # Near retirement or retired
        level = random.choices(['senior', 'management', 'special'], weights=[0.3, 0.3, 0.4])[0]
    
    # Generate position based on level
    if level == 'special':
        return random.choice(POSITION_LEVELS['special'])
    else:
        return random.choice(POSITION_LEVELS[level])

# =====================================================================================
# "residential_address", "work_address", "contact_address" data
# =====================================================================================
# Major Vietnamese provinces/cities with population weights
VIETNAM_LOCATIONS = {
    'TP. Hồ Chí Minh': {
        'weight': 0.15,  # 15% population
        'type': 'city',
        'districts': [
            'Quận 1', 'Quận 3', 'Quận 5', 'Quận 7', 'Quận 10', 'Quận 11',
            'Quận Bình Thạnh', 'Quận Tân Bình', 'Quận Phú Nhuận', 'Quận Gò Vấp',
            'Quận Bình Tân', 'Quận Tân Phú', 'Quận Thủ Đức', 'Huyện Củ Chi'
        ]
    },
    'Hà Nội': {
        'weight': 0.12,
        'type': 'city', 
        'districts': [
            'Quận Ba Đình', 'Quận Hoàn Kiếm', 'Quận Hai Bà Trưng', 'Quận Đống Đa',
            'Quận Tây Hồ', 'Quận Cầu Giấy', 'Quận Thanh Xuân', 'Quận Hoàng Mai',
            'Quận Long Biên', 'Quận Nam Từ Liêm', 'Quận Bắc Từ Liêm', 'Huyện Gia Lam'
        ]
    },
    'Đà Nẵng': {
        'weight': 0.04,
        'type': 'city',
        'districts': [
            'Quận Hải Châu', 'Quận Thanh Khê', 'Quận Sơn Trà', 'Quận Ngũ Hành Sơn',
            'Quận Liên Chiểu', 'Quận Cẩm Lệ', 'Huyện Hòa Vang'
        ]
    },
    'Cần Thơ': {
        'weight': 0.03,
        'type': 'city',
        'districts': [
            'Quận Ninh Kiều', 'Quận Bình Thuỷ', 'Quận Cái Răng', 'Quận Ô Môn',
            'Quận Thốt Nốt', 'Huyện Phong Điền', 'Huyện Cờ Đỏ'
        ]
    },
    'Hải Phòng': {
        'weight': 0.035,
        'type': 'city',
        'districts': [
            'Quận Hồng Bàng', 'Quận Ngô Quyền', 'Quận Lê Chân', 'Quận Hải An',
            'Quận Kiến An', 'Quận Đồ Sơn', 'Huyện An Dương'
        ]
    },
    'Thanh Hóa': {
        'weight': 0.055,
        'type': 'province',
        'districts': [
            'TP. Thanh Hóa', 'TX. Bỉm Sơn', 'TX. Sầm Sơn', 'Huyện Mường Lát',
            'Huyện Quan Hóa', 'Huyện Bá Thước', 'Huyện Quan Sơn'
        ]
    },
    'Nghệ An': {
        'weight': 0.053,
        'type': 'province', 
        'districts': [
            'TP. Vinh', 'TX. Cửa Lò', 'TX. Thái Hòa', 'Huyện Quế Phong',
            'Huyện Kỳ Sơn', 'Huyện Tương Dương', 'Huyện Nghĩa Đàn'
        ]
    },
    'Đồng Nai': {
        'weight': 0.04,
        'type': 'province',
        'districts': [
            'TP. Biên Hòa', 'TP. Long Khánh', 'Huyện Trảng Bom', 'Huyện Thống Nhất',
            'Huyện Vĩnh Cửu', 'Huyện Định Quán', 'Huyện Tân Phú'
        ]
    },
    'Bình Dương': {
        'weight': 0.035,
        'type': 'province',
        'districts': [
            'TP. Thủ Dầu Một', 'TX. Tân Uyên', 'TX. Dĩ An', 'TX. Thuận An',
            'Huyện Bàu Bàng', 'Huyện Dầu Tiếng', 'Huyện Phú Giáo'
        ]
    },
    'An Giang': {
        'weight': 0.03,
        'type': 'province',
        'districts': [
            'TP. Long Xuyên', 'TP. Châu Đốc', 'TX. Tân Châu', 'Huyện An Phú',
            'Huyện Tân Châu', 'Huyện Phú Tân', 'Huyện Châu Phú'
        ]
    },
    'Khánh Hòa': {
        'weight': 0.02,
        'type': 'province',
        'districts': [
            'TP. Nha Trang', 'TP. Cam Ranh', 'TX. Ninh Hòa', 'Huyện Diên Khánh',
            'Huyện Khánh Vĩnh', 'Huyện Cam Lâm', 'Huyện Vạn Ninh'
        ]
    },
    'Lâm Đồng': {
        'weight': 0.018,
        'type': 'province',
        'districts': [
            'TP. Đà Lạt', 'TP. Bảo Lộc', 'Huyện Đam Rông', 'Huyện Lạc Dương',
            'Huyện Lâm Hà', 'Huyện Đơn Dương', 'Huyện Đức Trọng'
        ]
    },
    'Bà Rịa - Vũng Tàu': {
        'weight': 0.017,
        'type': 'province',
        'districts': [
            'TP. Vũng Tàu', 'TP. Bà Rịa', 'TX. Phú Mỹ', 'Huyện Côn Đảo',
            'Huyện Châu Đức', 'Huyện Xuyên Mộc', 'Huyện Long Điền'
        ]
    },
    'Bình Định': {
        'weight': 0.025,
        'type': 'province',
        'districts': [
            'TP. Quy Nhon', 'TX. An Nhơn', 'TX. Hoài Nhơn', 'Huyện An Lão',
            'Huyện Hoài Ân', 'Huyện Phù Mỹ', 'Huyện Vĩnh Thạnh'
        ]
    },
    'Thừa Thiên Huế': {
        'weight': 0.022,
        'type': 'province',
        'districts': [
            'TP. Huế', 'TX. Hương Thủy', 'TX. Hương Trà', 'Huyện Phong Điền',
            'Huyện Quảng Điền', 'Huyện Phú Vang', 'Huyện A Lưới'
        ]
    },
    'Đắk Lắk': {
        'weight': 0.03,
        'type': 'province',
        'districts': [
            'TP. Buôn Ma Thuột', 'TX. Buôn Hồ', 'Huyện Ea H\'leo', 'Huyện Ea Sup',
            'Huyện Buôn Đôn', 'Huyện Cư M\'gar', 'Huyện Krông Búk'
        ]
    },
    'Bình Thuận': {
        'weight': 0.02,
        'type': 'province',
        'districts': [
            'TP. Phan Thiết', 'TP. La Gi', 'Huyện Tuy Phong', 'Huyện Bắc Bình',
            'Huyện Hàm Thuận Bắc', 'Huyện Hàm Thuận Nam', 'Huyện Tánh Linh'
        ]
    },
    'Đồng Tháp': {
        'weight': 0.025,
        'type': 'province',
        'districts': [
            'TP. Cao Lãnh', 'TP. Sa Đéc', 'TX. Hồng Ngự', 'Huyện Tân Hồng',
            'Huyện Châu Thành', 'Huyện Lấp Vò', 'Huyện Lai Vung'
        ]
    },
    'Kiên Giang': {
        'weight': 0.028,
        'type': 'province',
        'districts': [
            'TP. Rạch Giá', 'TP. Hà Tiên', 'TX. Dương Đông', 'Huyện Kiên Lương',
            'Huyện Hòn Đất', 'Huyện Tân Hiệp', 'Huyện Châu Thành'
        ]
    },
    'Cà Mau': {
        'weight': 0.02,
        'type': 'province',
        'districts': [
            'TP. Cà Mau', 'Huyện U Minh', 'Huyện Trần Văn Thời', 'Huyện Cái Nước',
            'Huyện Đầm Dơi', 'Huyện Năm Căn', 'Huyện Phú Tân'
        ]
    },
    'Sóc Trăng': {
        'weight': 0.022,
        'type': 'province',
        'districts': [
            'TP. Sóc Trăng', 'TX. Ngã Năm', 'TX. Vĩnh Châu', 'Huyện Châu Thành',
            'Huyện Kế Sách', 'Huyện Mỹ Tú', 'Huyện Cù Lao Dung'
        ]
    },
    'Bạc Liêu': {
        'weight': 0.015,
        'type': 'province',
        'districts': [
            'TP. Bạc Liêu', 'TX. Giá Rai', 'Huyện Hồng Dân', 'Huyện Phước Long',
            'Huyện Vĩnh Lợi', 'Huyện Đông Hải', 'Huyện Hòa Bình'
        ]
    },
    'Long An': {
        'weight': 0.025,
        'type': 'province',
        'districts': [
            'TP. Tân An', 'TX. Kiến Tường', 'Huyện Bến Lức', 'Huyện Thủ Thừa',
            'Huyện Tân Hưng', 'Huyện Vĩnh Hưng', 'Huyện Mộc Hóa'
        ]
    },
    'Tiền Giang': {
        'weight': 0.028,
        'type': 'province',
        'districts': [
            'TP. Mỹ Tho', 'TX. Gò Công', 'TX. Cai Lậy', 'Huyện Tân Phước',
            'Huyện Châu Thành', 'Huyện Chờ Gạo', 'Huyện Gò Công Tây'
        ]
    },
    'Bến Tre': {
        'weight': 0.02,
        'type': 'province',
        'districts': [
            'TP. Bến Tre', 'Huyện Châu Thành', 'Huyện Chợ Lách', 'Huyện Mỏ Cày Nam',
            'Huyện Giồng Trôm', 'Huyện Bình Đại', 'Huyện Ba Tri'
        ]
    },
    'Vĩnh Long': {
        'weight': 0.017,
        'type': 'province',
        'districts': [
            'TP. Vĩnh Long', 'Huyện Long Hồ', 'Huyện Mang Thít', 'Huyện Vũng Liêm',
            'Huyện Tam Bình', 'Huyện Bình Minh', 'Huyện Trà Ôn'
        ]
    }
}

# Common Vietnamese street names
VIETNAMESE_STREETS = [
    'Nguyễn Trái', 'Lê Lợi', 'Trần Hưng Đạo', 'Lý Thái Tổ', 'Hai Bà Trưng',
    'Nguyễn Huệ', 'Lê Duẩn', 'Nguyễn Thái Học', 'Phan Chu Trinh', 'Điện Biên Phủ',
    'Quang Trung', 'Lê Thánh Tôn', 'Nguyễn Du', 'Tôn Đức Thắng', 'Võ Văn Kiệt',
    'Cách Mạng Tháng Tám', 'Lý Tự Trọng', 'Nguyễn Văn Cừ', 'Phạm Văn Đồng',
    'Hoàng Văn Thụ', 'Nguyễn Ái Quốc', 'Lê Văn Lương', 'Thống Nhất', 'Cộng Hòa'
]

# Ward/Commune patterns
WARD_PATTERNS = {
    'urban': ['Phường {}', 'Phường Tân {}', 'Phường {}'],
    'suburban': ['Phường {}', 'Xã {}', 'Thị trấn {}'],
    'rural': ['Xã {}', 'Xã Tân {}', 'Xã {}']
}

def generate_residential_address():
    """
    Generate residential address with realistic Vietnamese distribution
    
    Returns:
        str: Full residential address
    """
    # Select province/city based on population weight
    provinces = list(VIETNAM_LOCATIONS.keys())
    weights = [VIETNAM_LOCATIONS[p]['weight'] for p in provinces]
    selected_province = random.choices(provinces, weights=weights)[0]
    
    # Select district from chosen province
    province_data = VIETNAM_LOCATIONS[selected_province]
    district = random.choice(province_data['districts'])
    
    # Generate ward based on location type
    location_type = province_data['type']
    if location_type == 'city':
        ward_type = random.choices(['urban', 'suburban'], weights=[0.7, 0.3])[0]
    else:
        ward_type = random.choices(['suburban', 'rural'], weights=[0.4, 0.6])[0]
    
    # Generate ward name
    ward_numbers = ['1', '2', '3', '4', '5', 'An', 'Bình', 'Hòa', 'Thành', 'Phước']
    ward_pattern = random.choice(WARD_PATTERNS[ward_type])
    ward = ward_pattern.format(random.choice(ward_numbers))
    
    # Generate street address
    house_number = random.randint(1, 999)
    street = random.choice(VIETNAMESE_STREETS)
    
    # Format full address
    address = f"{house_number} {street}, {ward}, {district}, {selected_province}"
    return address

def generate_work_address(occupation, residential_address):
    """
    Generate work address based on occupation and residential location
    
    Args:
        occupation (str): Person's occupation
        residential_address (str): Person's residential address
        
    Returns:
        str: Work address
    """
    # Extract province from residential address
    residential_province = residential_address.split(', ')[-1]
    
    # Occupation-based work location logic
    if any(keyword in occupation.lower() for keyword in ['nông dân', 'chăn nuôi', 'lâm nghiệp']):
        # Agricultural workers: 95% same province
        if random.random() < 0.95:
            return _generate_address_in_province(residential_province, prefer_rural=True)
        else:
            return generate_residential_address()
    
    elif any(keyword in occupation.lower() for keyword in ['ngư dân']):
        # Fishermen: Must be in coastal provinces
        coastal_provinces = [
            'TP. Hồ Chí Minh', 'Đà Nẵng', 'Hải Phòng', 'Khánh Hòa', 
            'Bà Rịa - Vũng Tàu', 'Bình Định', 'Thừa Thiên Huế', 
            'Bình Thuận', 'Kiên Giang', 'Cà Mau', 'Sóc Trăng', 'Bạc Liêu',
            'Tiền Giang', 'Bến Tre'
        ]
        selected = random.choice(coastal_provinces)
        return _generate_address_in_province(selected, prefer_rural=True)
    
    elif any(keyword in occupation.lower() for keyword in ['kỹ sư it', 'kỹ sư phần mềm', 'freelancer it']):
        # IT workers: 80% in major tech cities
        if random.random() < 0.8:
            tech_cities = ['TP. Hồ Chí Minh', 'Hà Nội', 'Đà Nẵng']
            weights = [0.6, 0.25, 0.15]
            selected = random.choices(tech_cities, weights=weights)[0]
            return _generate_address_in_province(selected, prefer_urban=True)
        else:
            return generate_residential_address()
    
    elif any(keyword in occupation.lower() for keyword in ['công chức', 'viên chức']):
        # Government workers: 90% same or nearby province
        if random.random() < 0.9:
            return _generate_address_in_province(residential_province, prefer_urban=True)
        else:
            return generate_residential_address()
    
    elif any(keyword in occupation.lower() for keyword in ['sinh viên']):
        # Students: University cities
        university_cities = ['TP. Hồ Chí Minh', 'Hà Nội', 'Đà Nẵng', 'Cần Thơ', 'Hải Phòng']
        weights = [0.4, 0.3, 0.15, 0.1, 0.05]
        selected = random.choices(university_cities, weights=weights)[0]
        return _generate_address_in_province(selected, prefer_urban=True)
    
    elif any(keyword in occupation.lower() for keyword in ['hưu trí', 'nội trợ']):
        # Retired/Housewife: Same as residential
        return residential_address
        
    else:
        # Other occupations: 70% same province, 30% different
        if random.random() < 0.7:
            return _generate_address_in_province(residential_province)
        else:
            return generate_residential_address()

def _generate_address_in_province(province_name, prefer_urban=False, prefer_rural=False):
    """
    Generate address within specific province
    
    Args:
        province_name (str): Target province
        prefer_urban (bool): Prefer urban areas
        prefer_rural (bool): Prefer rural areas
        
    Returns:
        str: Address in specified province
    """
    if province_name not in VIETNAM_LOCATIONS:
        # Fallback to random address
        return generate_residential_address()
    
    province_data = VIETNAM_LOCATIONS[province_name]
    district = random.choice(province_data['districts'])
    
    # Determine ward type based on preferences
    location_type = province_data['type']
    if prefer_urban:
        ward_type = 'urban'
    elif prefer_rural:
        ward_type = 'rural'
    else:
        if location_type == 'city':
            ward_type = random.choices(['urban', 'suburban'], weights=[0.7, 0.3])[0]
        else:
            ward_type = random.choices(['suburban', 'rural'], weights=[0.4, 0.6])[0]
    
    # Generate ward
    ward_numbers = ['1', '2', '3', '4', '5', 'An', 'Bình', 'Hòa', 'Thành', 'Phước']
    ward_pattern = random.choice(WARD_PATTERNS[ward_type])
    ward = ward_pattern.format(random.choice(ward_numbers))
    
    # Generate street address
    house_number = random.randint(1, 999)
    street = random.choice(VIETNAMESE_STREETS)
    
    return f"{house_number} {street}, {ward}, {district}, {province_name}"

def generate_contact_address(residential_address, work_address, age):
    """
    Generate contact address based on residential and work addresses
    
    Args:
        residential_address (str): Residential address
        work_address (str): Work address  
        age (int): Person's age
        
    Returns:
        str: Contact address
    """
    # Age-based logic for contact preference
    if age < 25:
        # Young people: 60% residential, 30% work, 10% other
        choices = [residential_address, work_address, 'other']
        weights = [0.6, 0.3, 0.1]
    elif age < 50:
        # Working age: 70% residential, 20% work, 10% other  
        choices = [residential_address, work_address, 'other']
        weights = [0.7, 0.2, 0.1]
    else:
        # Older people: 80% residential, 10% work, 10% other
        choices = [residential_address, work_address, 'other']
        weights = [0.8, 0.1, 0.1]
    
    selected = random.choices(choices, weights=weights)[0]
    
    if selected == 'other':
        # Generate different address (family/relatives)
        return generate_residential_address()
    else:
        return selected

# =====================================================================================
# "pin", "password" data
# =====================================================================================
def generate_pin_hash(full_name, date_of_birth, phone_number):
    """
    Generate PIN based on realistic user patterns, then hash it immediately
    
    Args:
        full_name (str): Customer's full name
        date_of_birth (date): Customer's birth date
        phone_number (str): Customer's phone number
        
    Returns:
        str: SHA-256 hashed PIN (no raw PIN stored)
    """
    
    # Realistic PIN pattern distribution based on Vietnamese user behavior
    patterns = ['weak', 'dates', 'sequences', 'repeated', 'random']
    weights = [0.15, 0.25, 0.20, 0.10, 0.30]
    pattern = random.choices(patterns, weights=weights)[0]
    
    if pattern == 'weak':
        # Common weak PINs in Vietnam
        raw_pin = random.choice(['123456', '000000', '111111', '654321', '123123'])
        
    elif pattern == 'dates':
        # Date-based PINs (popular in Vietnam)
        if random.random() < 0.6:
            # Birth date: DDMMYY format
            raw_pin = date_of_birth.strftime('%d%m%y')
        else:
            # Birth year + month: YYYYMM or YYMMDD
            if random.random() < 0.5:
                raw_pin = date_of_birth.strftime('%y%m%d')
            else:
                # Last 6 digits of birth year + random
                year_str = str(date_of_birth.year)
                raw_pin = year_str[-2:] + f"{random.randint(1, 99):02d}" + f"{random.randint(1, 99):02d}"
                
    elif pattern == 'sequences':
        # Sequential patterns
        sequences = ['123456', '654321', '135790', '246810', '987654', '112233']
        raw_pin = random.choice(sequences)
        
    elif pattern == 'repeated':
        # Repeated digits
        digit = random.choice('0123456789')
        raw_pin = digit * 6
        
    else:  # random
        # True random PIN (most secure)
        raw_pin = ''.join([secrets.choice('0123456789') for _ in range(6)])
    
    # Hash PIN using SHA-256 with salt
    # Generate unique salt for each PIN
    salt = secrets.token_hex(16)  # 32-character hex salt
    pin_with_salt = raw_pin + salt
    
    # Create hash
    hashed_pin = hashlib.sha256(pin_with_salt.encode('utf-8')).hexdigest()
    
    # Return format: salt$hash (for verification later)
    return f"{salt}${hashed_pin}"

def generate_password_hash(full_name, date_of_birth, phone_number):
    """
    Generate password based on realistic Vietnamese user patterns, then hash it
    
    Args:
        full_name (str): Customer's full name
        date_of_birth (date): Customer's birth date  
        phone_number (str): Customer's phone number
        
    Returns:
        tuple: (hashed_password, password_last_changed)
            - hashed_password (str): SHA-256 hashed password
            - password_last_changed (datetime): When password was last changed
    """
    
    # Realistic password pattern distribution for Vietnamese users
    patterns = ['name_based', 'date_based', 'common_weak', 'phone_based', 'random_strong']
    weights = [0.25, 0.20, 0.15, 0.15, 0.25]
    pattern = random.choices(patterns, weights=weights)[0]
    
    if pattern == 'name_based':
        # Name + numbers (popular in Vietnam)
        name_part = remove_vietnamese_diacritics(full_name.split()[-1].lower())  # Last name
        if random.random() < 0.6:
            # Name + birth year
            raw_password = name_part + str(date_of_birth.year)
        else:
            # Name + random numbers
            raw_password = name_part + str(random.randint(100, 9999))
            
    elif pattern == 'date_based':
        # Date combinations
        if random.random() < 0.4:
            # DDMMYYYY format
            raw_password = date_of_birth.strftime('%d%m%Y')
        elif random.random() < 0.7:
            # Name + DDMM
            name_part = remove_vietnamese_diacritics(full_name.split()[-1].lower())
            raw_password = name_part + date_of_birth.strftime('%d%m')
        else:
            # Birth year variations
            raw_password = str(date_of_birth.year) + str(random.randint(100, 999))
            
    elif pattern == 'common_weak':
        # Common weak passwords in Vietnam
        weak_passwords = [
            '123456', '123456789', 'password', 'qwerty123', 'vietnam123',
            'saigon123', 'hanoi123', '111111', 'abc123', 'password123'
        ]
        raw_password = random.choice(weak_passwords)
        
    elif pattern == 'phone_based':
        # Phone number variations
        phone_digits = ''.join(filter(str.isdigit, phone_number))
        if random.random() < 0.5:
            # Last 6-8 digits
            raw_password = phone_digits[-8:] if len(phone_digits) >= 8 else phone_digits
        else:
            # Phone + random chars
            raw_password = phone_digits[-6:] + str(random.randint(10, 99))
            
    else:  # random_strong
        # Strong random password (secure users)
        import string
        length = random.randint(8, 12)
        chars = string.ascii_letters + string.digits
        raw_password = ''.join(secrets.choice(chars) for _ in range(length))
    
    # Ensure minimum length
    if len(raw_password) < 6:
        raw_password = raw_password + str(random.randint(100, 999))
    
    # Hash password using SHA-256 with salt
    salt = secrets.token_hex(16)  # 32-character hex salt
    password_with_salt = raw_password + salt
    
    # Create hash  
    hashed_password = hashlib.sha256(password_with_salt.encode('utf-8')).hexdigest()
    
    # Generate password_last_changed timestamp (current time when password is created)
    from datetime import datetime
    password_last_changed = datetime.now()
    
    # Return tuple: (hash, timestamp)
    return f"{salt}${hashed_password}", password_last_changed


# =====================================================================================
# "risk_score", "risk_rating" data
# =====================================================================================
# Occupation risk scoring (lower scores for realistic distribution)
OCCUPATION_RISK = {
    # Very Low Risk (0-3 points) - Stable government/professional jobs
    'Công chức nhà nước': 0, 'Viên chức y tế': 1, 'Viên chức giáo dục': 1,
    'Công chức thuế': 0, 'Công chức hải quan': 1, 'Công chức công an': 0,
    'Bác sĩ': 1, 'Dược sĩ': 2, 'Nhân viên ngân hàng': 0,
    
    # Low Risk (3-6 points) - Professional/Technical jobs
    'Kỹ sư phần mềm': 3, 'Kỹ sư IT': 3, 'Kỹ sư xây dựng': 4,
    'Kỹ sư cơ khí': 4, 'Kỹ sư điện': 4, 'Kỹ sư hóa học': 5,
    'Giáo viên tiểu học': 2, 'Giáo viên THCS': 2, 'Giáo viên THPT': 3,
    'Giảng viên đại học': 2, 'Y tá': 3, 'Điều dưỡng': 3,
    
    # Medium Risk (6-10 points) - Regular employment
    'Chuyên viên tài chính': 6, 'Kế toán': 5, 'Nhân viên bán hàng': 7,
    'Nhân viên marketing': 7, 'Chuyên viên đầu tư': 8,
    'Kỹ thuật viên y tế': 6, 'Kỹ thuật viên nông nghiệp': 8,
    'Nhân viên khách sạn': 7, 'Nhân viên nhà hàng': 8,
    
    # Higher Risk (10-15 points) - Manual labor/Unstable income
    'Công nhân nhà máy': 10, 'Thợ điện': 11, 'Thợ máy': 11,
    'Công nhân xây dựng': 12, 'Thợ hàn': 12, 'Công nhân dệt may': 10,
    'Tài xế': 13, 'Bảo vệ': 11, 'Thợ làm tóc': 12, 'Masseur': 13,
    
    # High Risk (15+ points) - Irregular income/High risk sectors
    'Nông dân': 15, 'Ngư dân': 16, 'Chăn nuôi': 15, 'Lâm nghiệp': 16,
    'Freelancer IT': 18, 'Photographer': 18, 'Nhà thiết kế': 17,
    'Blogger/Youtuber': 20, 'Dịch thuật viên': 16, 'Gia sư': 17,
    
    # Special Cases
    'Sinh viên': 12, 'Hưu trí': 8, 'Nội trợ': 5, 'Thất nghiệp': 25
}

# Province risk scoring (border/remote = higher risk)
PROVINCE_RISK = {
    # Major cities (0-1 points)
    'TP. Hồ Chí Minh': 0, 'Hà Nội': 0, 'Đà Nẵng': 1,
    'Cần Thơ': 1, 'Hải Phòng': 1,
    
    # Large provinces (1-2 points)
    'Thanh Hóa': 1, 'Nghệ An': 1, 'Đồng Nai': 1, 'Bình Dương': 1,
    'Khánh Hòa': 1, 'Lâm Đồng': 2, 'Bà Rịa - Vũng Tàu': 1,
    'Bình Định': 2, 'Thừa Thiên Huế': 1, 'Đắk Lắk': 2,
    
    # Medium risk provinces (2-4 points) 
    'Bình Thuận': 2, 'Đồng Tháp': 2, 'Long An': 2, 'Tiền Giang': 2,
    'Bến Tre': 3, 'Vĩnh Long': 3, 'Sóc Trăng': 3, 'Bạc Liêu': 3,
    
    # Border/Remote provinces (4-6 points)
    'An Giang': 4, 'Kiên Giang': 3, 'Cà Mau': 4
}

def calculate_age(date_of_birth):
    """Calculate current age from date of birth"""
    today = date.today()
    return today.year - date_of_birth.year - ((today.month, today.day) < (date_of_birth.month, date_of_birth.day))

def extract_province(address):
    """Extract province from Vietnamese address (last part after last comma)"""
    return address.split(', ')[-1].strip()

def is_phone_valid(phone_number):
    """Check if phone number follows valid Vietnamese format"""
    # Valid: 0xxxxxxxxx with correct prefix and exactly 10 digits
    if len(phone_number) != 10 or not phone_number.startswith('0'):
        return False
    
    # Check if prefix is valid Vietnamese prefix
    prefix = phone_number[1:3]
    valid_prefixes = [
        '86', '96', '97', '98', '32', '33', '34', '35', '36', '37', '38', '39',
        '88', '91', '94', '83', '84', '85', '81', '82',
        '89', '90', '93', '70', '79', '77', '76', '78',
        '92', '56', '58', '99', '59'
    ]
    return prefix in valid_prefixes

def is_tax_id_valid(tax_id):
    """Check if tax ID follows valid Vietnamese format"""
    # Valid: 10 digits (personal) or 13 digits (business)
    return len(tax_id) in [10, 13] and tax_id.isdigit()

def is_id_valid(id_number, doc_type):
    """Check if ID/Passport follows valid format"""
    if doc_type == 'CCCD':
        # Valid CCCD: exactly 12 digits
        return len(id_number) == 12 and id_number.isdigit()
    else:  # Passport
        # Valid Passport: 1 letter + 7 digits
        return len(id_number) == 8 and id_number[0].isalpha() and id_number[1:].isdigit()

def calculate_risk_score_and_rating(customer_data):
    """
    Calculate comprehensive risk score and rating for customer
    
    Args:
        customer_data (dict): Customer information dictionary
        
    Returns:
        tuple: (risk_score, risk_rating)
    """
    base_score = 5.0  # Lower baseline for realistic distribution
    
    # 1. Occupation risk (40% weight) - Most important factor
    occupation = customer_data.get('occupation', '')
    occupation_risk = OCCUPATION_RISK.get(occupation, 10)  # Default medium risk
    
    # 2. Age risk (15% weight)
    age = customer_data.get('age', 30)
    if age < 21:
        age_risk = 8      # Young, inexperienced
    elif age < 25:
        age_risk = 5      # Young adult
    elif age < 60:
        age_risk = 0      # Prime working age (lowest risk)
    else:
        age_risk = 3      # Elderly, but stable
    
    # 3. Document & Residency risk (20% weight)
    doc_type = customer_data.get('document_type', 'CCCD')
    is_resident = customer_data.get('is_resident', True)
    
    if doc_type == 'CCCD' and is_resident:
        document_risk = 0    # Vietnamese citizen with verified address
    elif doc_type == 'CCCD' and not is_resident:
        document_risk = 5    # Vietnamese but non-resident 
    elif doc_type == 'Passport' and not is_resident:
        document_risk = 8    # Foreigner (higher monitoring)
    else:  # Passport + resident
        document_risk = 3    # Viet Kieu or naturalized
    
    # 4. Contact verification risk (10% weight)
    phone_valid = customer_data.get('phone_valid', True)
    has_email = customer_data.get('has_email', False)
    
    contact_risk = 0
    if not phone_valid:
        contact_risk += 6    # Invalid phone increases risk
    if not has_email:
        contact_risk += 2    # No email slightly increases risk
        
    # 5. Geographic risk (10% weight)
    province = customer_data.get('province', 'TP. Hồ Chí Minh')
    geo_risk = PROVINCE_RISK.get(province, 2)  # Default low-medium risk
    
    # 6. Data completeness risk (5% weight)
    completeness_risk = 0
    if not customer_data.get('tax_id_valid', True):
        completeness_risk += 3
    if not customer_data.get('id_passport_valid', True):
        completeness_risk += 2
    
    # Calculate total score
    total_score = (base_score + 
                  occupation_risk + 
                  age_risk + 
                  document_risk + 
                  contact_risk + 
                  geo_risk + 
                  completeness_risk)
    
    # Clamp to 0-100 range
    risk_score = min(max(total_score, 0.0), 100.0)
    
    # Assign risk rating (adjusted thresholds for realistic distribution)
    if risk_score <= 30:
        risk_rating = 'Low'      
    elif risk_score <= 60:
        risk_rating = 'Medium'   
    else:
        risk_rating = 'High'     
    
    return round(risk_score, 2), risk_rating

# =====================================================================================
# "customer_type", "monthly_income", "status" data
# =====================================================================================
def generate_customer_type():
    """
    Generate customer type with realistic distribution
    
    Returns:
        str: 'Individual' (90%) or 'Organization' (10%)
    """
    return 'Individual' if random.random() < 0.9 else 'Organization'


def generate_monthly_income(occupation, age, province, customer_type):
    """
    Generate realistic monthly income based on occupation, age, location and customer type
    
    Args:
        occupation (str): Person's occupation
        age (int): Person's age
        province (str): Province/city location
        customer_type (str): 'Individual' or 'Organization'
        
    Returns:
        int: Monthly income in VND (Vietnamese Dong)
    """
    
    # Base income by occupation (VND millions per month)
    BASE_INCOME = {
        # Government/Public sector (stable but lower)
        'Công chức nhà nước': (8, 15),
        'Viên chức y tế': (10, 18), 
        'Viên chức giáo dục': (8, 14),
        'Công chức thuế': (12, 20),
        'Công chức hải quan': (10, 18),
        'Công chức công an': (9, 16),
        
        # Healthcare (good income)
        'Bác sĩ': (20, 50),
        'Y tá': (8, 15),
        'Dược sĩ': (12, 25),
        'Kỹ thuật viên y tế': (7, 12),
        'Điều dưỡng': (6, 12),
        'Bác sĩ răng hàm mặt': (25, 60),
        
        # Education 
        'Giáo viên tiểu học': (6, 12),
        'Giáo viên THCS': (7, 13),
        'Giáo viên THPT': (8, 15),
        'Giảng viên đại học': (15, 35),
        'Giáo viên mầm non': (5, 10),
        'Huấn luyện viên': (8, 20),
        
        # Engineering/Tech (high income)
        'Kỹ sư phần mềm': (20, 60),
        'Kỹ sư IT': (18, 55),
        'Kỹ sư xây dựng': (12, 30),
        'Kỹ sư cơ khí': (10, 25),
        'Kỹ sư điện': (12, 28),
        'Kỹ sư hóa học': (15, 35),
        
        # Business/Finance (varies widely)
        'Nhân viên ngân hàng': (12, 35),
        'Kế toán': (8, 20),
        'Nhân viên bán hàng': (6, 25),
        'Chuyên viên tài chính': (15, 40),
        'Nhân viên marketing': (10, 30),
        'Chuyên viên đầu tư': (20, 80),
        
        # Service sector
        'Nhân viên khách sạn': (5, 12),
        'Nhân viên nhà hàng': (4, 10),
        'Tài xế': (6, 15),
        'Bảo vệ': (5, 8),
        'Thợ làm tóc': (4, 12),
        'Masseur': (5, 15),
        
        # Manufacturing/Manual labor
        'Công nhân nhà máy': (5, 12),
        'Thợ điện': (8, 18),
        'Thợ máy': (7, 16),
        'Công nhân xây dựng': (6, 14),
        'Thợ hàn': (8, 20),
        'Công nhân dệt may': (4, 8),
        
        # Agriculture/Primary sector (lower income)
        'Nông dân': (3, 8),
        'Ngư dân': (4, 12),
        'Chăn nuôi': (3, 10),
        'Kỹ thuật viên nông nghiệp': (6, 15),
        'Lâm nghiệp': (4, 10),
        
        # Freelance/Self-employed (high variance)
        'Freelancer IT': (10, 100),
        'Photographer': (5, 30),
        'Nhà thiết kế': (8, 40),
        'Blogger/Youtuber': (2, 50),
        'Dịch thuật viên': (8, 25),
        'Gia sư': (3, 15),
        
        # Special cases
        'Sinh viên': (0, 2),     # Part-time income or allowance
        'Hưu trí': (3, 8),       # Pension
        'Nội trợ': (0, 0),       # No income
        'Thất nghiệp': (0, 0),   # No income
        'Khởi nghiệp': (0, 50)   # Highly variable
    }
    
    # Get base income range
    income_range = BASE_INCOME.get(occupation, (5, 15))  # Default range
    min_income, max_income = income_range
    
    # Age multiplier (experience factor)
    if age < 25:
        age_multiplier = 0.6   # Entry level
    elif age < 30:
        age_multiplier = 0.8   # Junior
    elif age < 40:
        age_multiplier = 1.0   # Standard
    elif age < 50:
        age_multiplier = 1.3   # Senior
    elif age < 60:
        age_multiplier = 1.5   # Management
    else:
        age_multiplier = 1.2   # Near retirement
    
    # Location multiplier (cost of living)
    LOCATION_MULTIPLIER = {
        'TP. Hồ Chí Minh': 1.4,
        'Hà Nội': 1.3,
        'Đà Nẵng': 1.1,
        'Cần Thơ': 1.0,
        'Hải Phòng': 1.0,
        'Thanh Hóa': 0.8,
        'Nghệ An': 0.7,
        'Đồng Nai': 1.1,
        'Bình Dương': 1.1,
        'An Giang': 0.7,
        'Khánh Hòa': 0.9,
        'Lâm Đồng': 0.8,
        'Bà Rịa - Vũng Tàu': 1.0,
        'Bình Định': 0.8,
        'Thừa Thiên Huế': 0.8,
        'Đắk Lắk': 0.7,
        'Bình Thuận': 0.8,
        'Đồng Tháp': 0.7,
        'Kiên Giang': 0.7,
        'Cà Mau': 0.6,
        'Sóc Trăng': 0.6,
        'Bạc Liêu': 0.6,
        'Long An': 0.7,
        'Tiền Giang': 0.7,
        'Bến Tre': 0.6,
        'Vĩnh Long': 0.6
    }
    
    location_multiplier = LOCATION_MULTIPLIER.get(province, 0.8)
    
    # Customer type multiplier
    if customer_type == 'Organization':
        # Organizations typically have higher declared income
        type_multiplier = random.uniform(2.0, 5.0)
    else:
        type_multiplier = 1.0
    
    # Calculate income range
    adjusted_min = min_income * age_multiplier * location_multiplier * type_multiplier
    adjusted_max = max_income * age_multiplier * location_multiplier * type_multiplier
    
    # Generate random income within range
    if adjusted_min >= adjusted_max:
        base_income = adjusted_min
    else:
        base_income = random.uniform(adjusted_min, adjusted_max)
    
    # Convert to VND (millions -> actual VND)
    income_vnd = int(base_income * 1_000_000)
    
    # Round to nearest 100,000 VND for realism
    income_vnd = round(income_vnd / 100_000) * 100_000
    
    # Ensure minimum wage compliance (Vietnam 2024: ~4.7M VND/month)
    if customer_type == 'Individual' and income_vnd > 0:
        income_vnd = max(income_vnd, 4_700_000)
    
    return income_vnd

def generate_status():
    """
    Generate customer account status with realistic distribution
    
    Returns:
        str: Account status - 'Active' (90%), 'Closed' (4%), 'Suspended' (4%), 'Inactive' (2%)
    """
    rand = random.random()
    
    if rand < 0.85:
        return 'Active'
    elif rand < 0.94:
        return 'Closed'
    elif rand < 0.94:
        return 'Suspended'
    else:
        return 'Inactive'