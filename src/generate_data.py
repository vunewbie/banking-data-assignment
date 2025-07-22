from datetime import datetime, timedelta
from generate.generate_customer_data import *
from generate.generate_face_template_data import generate_face_encoding
from generate.generate_customer_device_data import *
from generate.generate_bank_account_data import *
from generate.generate_transaction_data import *
import random
import pandas as pd
import uuid

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

def get_time_based_seed():
    """Generate seed based on current timestamp"""
    return int(datetime.now().timestamp())

def generate_face_template_data(customer_df):
    """
    Generate face_template data based on existing customer DataFrame
    
    Args:
        customer_df (pd.DataFrame): DataFrame with customer data
        
    Returns:
        tuple: (face_template_df, updated_customer_df)
            - face_template_df: DataFrame with face template records
            - updated_customer_df: Updated customer DataFrame with kyc_completed_at and updated_at
    """
    print("Starting face template data generation...")
    
    # Business logic: 85% of customers complete KYC with face template
    kyc_completion_rate = 0.85
    
    face_templates = []
    kyc_completed_customers = []
    
    print(f"Processing {len(customer_df)} customers for face template generation...")
    
    for index, customer in customer_df.iterrows():
        customer_id = customer['customer_id']
        
        # Determine if this customer completes KYC (85% probability)
        if random.random() < kyc_completion_rate:
            # Generate face template for this customer
            face_template_id = str(uuid.uuid4())
            face_encoding = generate_face_encoding(customer_id)
            current_time = datetime.now()
            
            face_template = {
                'template_id': face_template_id,                   
                'customer_id': customer_id,
                'encrypted_face_encoding': face_encoding,          
                'created_at': current_time,
                'last_used_at': current_time                      
            }
            
            face_templates.append(face_template)
            kyc_completed_customers.append(customer_id)
            
        # Progress indicator
        if (index + 1) % 300 == 0 or index == len(customer_df) - 1:
            progress = ((index + 1) / len(customer_df)) * 100
            print(f"Progress: {index + 1}/{len(customer_df)} ({progress:.1f}%)")
    
    # Create face_template DataFrame
    face_template_df = pd.DataFrame(face_templates)
    
    # Update customer DataFrame
    updated_customer_df = customer_df.copy()
    current_time = datetime.now()
    
    # Update kyc_completed_at for customers with face templates
    for customer_id in kyc_completed_customers:
        mask = updated_customer_df['customer_id'] == customer_id
        updated_customer_df.loc[mask, 'kyc_completed_at'] = current_time
    
    # Update updated_at for all customers (KYC process attempted)
    updated_customer_df['updated_at'] = current_time
    
    print(f"Successfully generated {len(face_template_df)} face templates")
    print(f"KYC completion rate: {len(face_template_df)}/{len(customer_df)} ({len(face_template_df)/len(customer_df)*100:.1f}%)")
    print(f"Updated customer records with KYC completion status")
    
    return face_template_df, updated_customer_df

def generate_customer_device_data(customer_df):
    """
    Generate customer_device data based on existing customer DataFrame
    
    Args:
        customer_df (pd.DataFrame): DataFrame with customer data
        
    Returns:
        pd.DataFrame: DataFrame with customer device records
    """
    print("Starting customer device data generation...")
    
    # Reset device tracking for clean generation
    reset_device_identifier_tracking()
    
    devices = []
    
    print(f"Processing {len(customer_df)} customers for device generation...")
    
    for index, customer in customer_df.iterrows():
        customer_id = customer['customer_id']
        customer_age = calculate_age(customer['date_of_birth'])
        
        # Business logic: Device count per customer during onboarding
        # 85% have 1 device, 15% have 2 devices
        device_count = 1 if random.random() < 0.85 else 2
        
        for device_num in range(device_count):
            # Generate device data
            device_type = generate_device_type()
            device_identifier = generate_device_identifier(device_type, customer_id)
            device_name = generate_device_name(device_type)
            is_trusted = generate_is_trusted(device_num + 1, device_type)
            device_status = generate_device_status()
            
            # Timestamps for device registration during onboarding
            # Devices registered within 0-7 days after customer creation
            customer_created = customer['created_at']
            days_offset = random.randint(0, 7)
            hours_offset = random.randint(0, 23)
            minutes_offset = random.randint(0, 59)
            
            first_seen_at = customer_created + timedelta(
                days=days_offset, 
                hours=hours_offset, 
                minutes=minutes_offset
            )
            
            # Last used: somewhere between first_seen and now (for active devices)
            if device_status == 'Active':
                # Active devices used recently
                days_since_first = (datetime.now() - first_seen_at).days
                if days_since_first > 0:
                    last_used_offset = random.randint(0, min(days_since_first, 30))
                    last_used_at = datetime.now() - timedelta(days=last_used_offset)
                else:
                    last_used_at = first_seen_at + timedelta(hours=random.randint(1, 24))
            else:
                # Inactive devices: last used closer to first_seen
                last_used_at = first_seen_at + timedelta(days=random.randint(1, 7))
            
            device_record = {
                'device_identifier': device_identifier,  # PRIMARY KEY in schema
                'customer_id': customer_id,
                'device_type': device_type,
                'device_name': device_name,
                'is_trusted': is_trusted,
                'status': device_status,
                'first_seen_at': first_seen_at,
                'last_used_at': last_used_at
                # Removed: device_id, created_at, updated_at (not in schema)
            }
            
            devices.append(device_record)
        
        # Progress indicator
        if (index + 1) % 300 == 0 or index == len(customer_df) - 1:
            progress = ((index + 1) / len(customer_df)) * 100
            print(f"Progress: {index + 1}/{len(customer_df)} ({progress:.1f}%)")
    
    device_df = pd.DataFrame(devices)
    
    print(f"Successfully generated {len(device_df)} customer devices")
    print(f"Device distribution:")
    print(f"   - Total devices: {len(device_df)}")
    print(f"   - Customers with 1 device: {len(customer_df) - (len(device_df) - len(customer_df))}")
    print(f"   - Customers with 2 devices: {len(device_df) - len(customer_df)}")
    print(f"   - Average devices per customer: {len(device_df)/len(customer_df):.2f}")
    
    return device_df

def generate_authentication_log_data(customer_df, device_df):
    """
    Generate authentication_log data based on customers and their devices
    
    Args:
        customer_df (pd.DataFrame): DataFrame with customer data
        device_df (pd.DataFrame): DataFrame with device data
        
    Returns:
        pd.DataFrame: DataFrame with authentication log records
    """
    print("Starting authentication log data generation...")
    
    auth_logs = []
    
    # Group devices by customer for easier processing
    devices_by_customer = device_df.groupby('customer_id')
    
    print(f"Processing authentication logs for {len(customer_df)} customers...")
    
    for index, customer in customer_df.iterrows():
        customer_id = customer['customer_id']
        
        # Get devices for this customer
        try:
            customer_devices = devices_by_customer.get_group(customer_id)
        except KeyError:
            # No devices for this customer
            continue
        
        if len(customer_devices) == 0:
            continue
            
        # Generate authentication attempts for each device
        for _, device in customer_devices.iterrows():
            device_identifier = device['device_identifier']  
            first_seen = device['first_seen_at']
            last_used = device['last_used_at']
            device_status = device['status']
            is_trusted = device['is_trusted']
            
            # Number of authentication attempts based on device usage pattern
            if device_status == 'Active':
                # Active devices: 5-20 authentication attempts
                auth_count = random.randint(5, 20)
            elif device_status == 'Blocked':
                # Blocked devices: 3-8 attempts (some failed leading to block)
                auth_count = random.randint(3, 8)
            else:  # Expired/Inactive
                # Limited attempts
                auth_count = random.randint(1, 5)
            
            # Generate authentication attempts
            for attempt_num in range(auth_count):
                # Authentication method distribution
                auth_methods = ['PIN', 'Password', 'Biometric']
                if customer_id in customer_df[customer_df['kyc_completed_at'].notna()]['customer_id'].values:
                    # Customer has completed KYC - can use biometric
                    method_weights = [0.6, 0.25, 0.15]  # PIN preferred, some biometric
                else:
                    # No KYC - only PIN/Password
                    method_weights = [0.7, 0.3, 0.0]
                
                auth_method = random.choices(auth_methods, weights=method_weights)[0]
                if auth_method == 'Biometric' and auth_methods[2] == 'Biometric' and method_weights[2] == 0.0:
                    auth_method = 'PIN'  # Fallback if no biometric available
                
                # Success rate based on device trust and method
                if is_trusted:
                    base_success_rate = 0.95  # Trusted devices high success
                else:
                    base_success_rate = 0.85  # Untrusted devices lower success
                
                # Method-specific adjustments
                if auth_method == 'PIN':
                    success_rate = base_success_rate
                elif auth_method == 'Password':
                    success_rate = base_success_rate - 0.05  # Slightly harder
                else:  # Biometric
                    success_rate = base_success_rate + 0.03  # More reliable
                
                # Blocked devices have failed attempts leading to block
                if device_status == 'Blocked' and attempt_num >= auth_count - 2:
                    is_successful = False  # Last few attempts failed
                else:
                    is_successful = random.random() < success_rate
                
                # Generate timestamp between first_seen and last_used
                time_diff = last_used - first_seen
                time_range_seconds = time_diff.total_seconds()
                random_seconds = random.uniform(0, max(time_range_seconds, 3600))  # At least 1 hour range
                auth_timestamp = first_seen + timedelta(seconds=random_seconds)
                
                # IP address (simplified - Vietnamese ISP ranges)
                ip_prefixes = ['14.', '27.', '42.', '103.', '113.', '116.', '118.', '171.', '222.']
                ip_address = random.choice(ip_prefixes) + '.'.join([str(random.randint(0, 255)) for _ in range(3)])
                
                # User agent based on device type
                if device['device_type'] == 'Mobile':
                    user_agents = [
                        'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15',
                        'Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36',
                        'Mozilla/5.0 (Linux; Android 11; Redmi Note 10) AppleWebKit/537.36'
                    ]
                else:  # Desktop/Tablet
                    user_agents = [
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
                    ]
                
                user_agent = random.choice(user_agents)
                
                # Generate additional auth log fields per schema
                status = 'Success' if is_successful else random.choice(['Failed', 'Blocked', 'Timeout'])
                failure_reason = None if is_successful else random.choice([
                    'Invalid credentials', 'Too many attempts', 'Device not recognized', 'Session expired'
                ])
                
                # Map auth method to schema authentication_type
                auth_type_mapping = {
                    'PIN': 'Transaction_PIN',
                    'Password': 'Login_Password', 
                    'Biometric': 'Login_Biometric'
                }
                auth_type = auth_type_mapping.get(auth_method, 'Login_Password')
                
                auth_log = {
                    'log_id': str(uuid.uuid4()),
                    'customer_id': customer_id,
                    'device_identifier': device_identifier,  
                    'authentication_type': auth_type,        
                    'transaction_id': None,                  
                    'ip_address': ip_address,
                    'status': status,                       
                    'failure_reason': failure_reason,
                    'otp_sent_to': None,                     
                    'biometric_score': round(random.uniform(0.85, 0.99), 4) if 'Biometric' in auth_method else None,
                    'attempt_count': 1,
                    'session_id': str(uuid.uuid4())[:16],    
                    'created_at': auth_timestamp
                }
                
                auth_logs.append(auth_log)
        
        # Progress indicator
        if (index + 1) % 300 == 0 or index == len(customer_df) - 1:
            progress = ((index + 1) / len(customer_df)) * 100
            print(f"Progress: {index + 1}/{len(customer_df)} ({progress:.1f}%)")
    
    auth_log_df = pd.DataFrame(auth_logs)
    total_attempts = len(auth_log_df)
    successful_attempts = len(auth_log_df[auth_log_df['status'] == 'Success'])
    success_rate = (successful_attempts / total_attempts * 100) if total_attempts > 0 else 0
    
    print(f"Successfully generated {total_attempts} authentication log records")
    print(f"Authentication statistics:")
    print(f"   - Total attempts: {total_attempts}")
    print(f"   - Successful: {successful_attempts} ({success_rate:.1f}%)")
    print(f"   - Failed: {total_attempts - successful_attempts} ({100 - success_rate:.1f}%)")
    
    method_counts = auth_log_df['authentication_type'].value_counts()
    print(f"   - Method distribution:")
    for method, count in method_counts.items():
        percentage = (count / total_attempts * 100) if total_attempts > 0 else 0
        print(f"     * {method}: {count} ({percentage:.1f}%)")
    
    return auth_log_df

def generate_bank_account_data(customer_df, device_df):
    """
    Generate bank_account data based on customers and their verified devices
    
    Args:
        customer_df (pd.DataFrame): DataFrame with customer data
        device_df (pd.DataFrame): DataFrame with device data
        
    Returns:
        pd.DataFrame: DataFrame with bank account records
    """
    print("Starting bank account data generation...")
    
    # Reset account tracking
    from generate.generate_bank_account_data import _used_account_numbers
    _used_account_numbers.clear()
    
    bank_accounts = []
    
    # Only create accounts for customers who have registered devices
    customers_with_devices = device_df['customer_id'].unique()
    eligible_customers = customer_df[customer_df['customer_id'].isin(customers_with_devices)]
    
    print(f"Processing {len(eligible_customers)} customers with devices for bank account creation...")
    
    for index, customer in eligible_customers.iterrows():
        customer_id = customer['customer_id']
        customer_type = customer['customer_type']
        
        # Business logic: Account count per customer
        # 80% have 1 account, 20% have 2 accounts
        account_count = 1 if random.random() < 0.8 else 2
        
        for account_num in range(account_count):
            # Generate account data using NEW FUNCTIONS
            account_type = generate_account_type()
            account_number = generate_account_number(customer_id, account_type)
            currency = generate_account_currency(customer_type)  # Use bank account currency function
            
            # Generate balance information using new function (schema-compliant)
            balance_info = generate_balance_info(account_type, customer['monthly_income'])
            
            # Generate limits based on customer profile
            daily_transfer_limit = generate_daily_transfer_limit(account_type, customer_type)
            daily_online_payment_limit = generate_daily_online_payment_limit(daily_transfer_limit)
            
            # Account status and flags
            account_status = generate_account_status()
            is_primary = generate_is_primary(account_num)  
            
            # Generate additional schema fields
            is_online_payment_enabled = generate_is_online_payment_enabled(account_type, account_status)
            interest_rate = generate_interest_rate(account_type)
            
            # Account creation timestamp (after device registration)
            customer_devices = device_df[device_df['customer_id'] == customer_id]
            earliest_device_time = customer_devices['first_seen_at'].min()
            
            # Account created 1-3 days after first device registration
            days_offset = random.randint(1, 3)
            hours_offset = random.randint(0, 23)
            account_open_at = earliest_device_time + timedelta(days=days_offset, hours=hours_offset)
            
            # Generate last transaction timestamp
            last_transaction_at = generate_last_transaction_at(account_open_at, account_status)
            
            bank_account = {
                'account_id': str(uuid.uuid4()),
                'customer_id': customer_id,
                'account_number': account_number,
                'account_type': account_type,
                'currency': currency,
                'available_balance': balance_info['available_balance'],
                'current_balance': balance_info['current_balance'], 
                'hold_amount': balance_info['hold_amount'],
                'daily_transfer_limit': daily_transfer_limit,
                'daily_online_payment_limit': daily_online_payment_limit,
                'is_primary': is_primary,
                'status': account_status,
                'is_online_payment_enabled': is_online_payment_enabled,
                'interest_rate': interest_rate,
                'last_transaction_at': last_transaction_at,
                'open_at': account_open_at,
                'updated_at': account_open_at                      
            }
            
            bank_accounts.append(bank_account)
        
        # Progress indicator
        if (index + 1) % 300 == 0 or index == len(eligible_customers) - 1:
            progress = ((index + 1) / len(eligible_customers)) * 100
            print(f"Progress: {index + 1}/{len(eligible_customers)} ({progress:.1f}%)")
    
    bank_account_df = pd.DataFrame(bank_accounts)
    
    print(f"Successfully generated {len(bank_account_df)} bank accounts")
    print(f"Account distribution:")
    print(f"   - Total accounts: {len(bank_account_df)}")
    print(f"   - Customers with accounts: {len(eligible_customers)}")
    print(f"   - Average accounts per customer: {len(bank_account_df)/len(eligible_customers):.2f}")
    
    return bank_account_df

def generate_transaction_data(customer_df, bank_account_df, device_df, face_template_df):
    """
    Generate transaction data based on bank accounts and customer activity
    
    Args:
        customer_df (pd.DataFrame): DataFrame with customer data
        bank_account_df (pd.DataFrame): DataFrame with bank account data
        device_df (pd.DataFrame): DataFrame with device data
        face_template_df (pd.DataFrame): DataFrame with face template data
        
    Returns:
        tuple: (transaction_df, transaction_auth_log_df)
    """
    print("Starting transaction data generation...")
    
    transactions = []
    transaction_auth_logs = []
    
    # Get customers with KYC completion for biometric capability
    customers_with_biometric = set(face_template_df['customer_id'].unique())
    
    print(f"Processing {len(bank_account_df)} bank accounts for transaction generation...")
    
    for index, account in bank_account_df.iterrows():
        customer_id = account['customer_id']
        account_id = account['account_id']
        account_balance = account['current_balance']  
        
        # Get customer info
        customer = customer_df[customer_df['customer_id'] == customer_id].iloc[0]
        customer_income = customer['monthly_income']
        has_biometric = customer_id in customers_with_biometric
        
        # Get customer's devices
        customer_devices = device_df[device_df['customer_id'] == customer_id]
        
        if len(customer_devices) == 0:
            continue
        
        # Generate 10-50 transactions per account over the past month
        transaction_count = random.randint(10, 50)
        daily_transaction_total = 0  # Track daily total for strong auth requirement
        current_day = None
        
        for trans_num in range(transaction_count):
            # Select random device for transaction
            device = customer_devices.sample(1).iloc[0]
            device_identifier = device['device_identifier']  
            device_trusted = device['is_trusted']
            transaction_type = generate_transaction_type()  
            amount = generate_transaction_amount(transaction_type, customer_income)
            currency = generate_transaction_currency()  
            fee = generate_fee(transaction_type, amount)  
            note = generate_note(transaction_type, amount)  
            auth_method = generate_authentication_method(amount, device_trusted)  
            recipient_info = generate_recipient_info(transaction_type)  
            bill_info = generate_bill_payment_info(transaction_type)  
            fraud_info = generate_fraud_detection_info(amount, auth_method, device_trusted)  
            
            # Generate transaction timestamp (past 30 days)
            days_ago = random.randint(0, 30)
            hours_ago = random.randint(0, 23)
            minutes_ago = random.randint(0, 59)
            transaction_time = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
            
            # Track daily totals for strong auth requirement
            transaction_date = transaction_time.date()
            if current_day != transaction_date:
                current_day = transaction_date
                daily_transaction_total = 0
            daily_transaction_total += amount
            
            # Generate transaction status
            transaction_status = generate_transaction_status(auth_method, has_biometric)  # Updated params
            
            # Generate completion timestamp
            completed_at = generate_completed_at(transaction_time, transaction_status)  # NEW: completed_at field
            
            transaction_record = {
                'transaction_id': str(uuid.uuid4()),
                'account_id': account_id,
                'transaction_type': transaction_type,
                'amount': amount,
                'currency': currency,                                                    
                'fee': fee,                                                             
                'status': transaction_status,
                'note': note,                                                           
                'authentication_method': auth_method,                                  
                # Recipient info (conditional based on transaction type)
                'recipient_account_number': recipient_info['recipient_account_number'], 
                'recipient_bank_code': recipient_info['recipient_bank_code'],           
                'recipient_name': recipient_info['recipient_name'],
                # Bill payment info (conditional based on transaction type)
                'service_provider_code': bill_info['service_provider_code'],         
                'bill_number': bill_info['bill_number'],                               
                # Fraud detection info
                'is_fraud': fraud_info['is_fraud'],                                    
                'fraud_score': fraud_info['fraud_score'],                              
                # Timestamps
                'created_at': transaction_time,
                'completed_at': completed_at                                           
                # REMOVED: device_id, balance_before, balance_after, updated_at (not in schema)
            }
            
            transactions.append(transaction_record)
            
            # Generate authentication log for this transaction (single method now)
                # Success rate based on method and setup
            if 'Biometric' in auth_method and not has_biometric:
                    is_successful = False  # Can't use biometric without setup
            elif transaction_status == 'Failed':
                    is_successful = False  # Transaction failed, auth failed
            else:
                # Normal success rates based on method type
                if 'PIN' in auth_method:
                    base_rate = 0.95
                elif 'OTP' in auth_method:
                    base_rate = 0.92
                elif 'Biometric' in auth_method:
                    base_rate = 0.98
                else:
                    base_rate = 0.95
                is_successful = random.random() < base_rate
                
            # Generate IP and user agent (regardless of success/failure)
                ip_prefixes = ['14.', '27.', '42.', '103.', '113.', '116.', '118.', '171.', '222.']
                ip_address = random.choice(ip_prefixes) + '.'.join([str(random.randint(0, 255)) for _ in range(3)])
                
                if device['device_type'] == 'Mobile':
                    user_agents = [
                        'Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15',
                        'Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36',
                        'Mozilla/5.0 (Linux; Android 11; Redmi Note 10) AppleWebKit/537.36'
                    ]
                else:
                    user_agents = [
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
                    ]
                
            # Generate additional auth log fields per schema
            status = 'Success' if is_successful else random.choice(['Failed', 'Blocked', 'Timeout'])
            failure_reason = None if is_successful else random.choice([
                'Insufficient funds', 'Invalid PIN', 'OTP expired', 'Biometric mismatch'
            ])
            
            # Map auth method to schema authentication_type  
            auth_type_mapping = {
                'PIN': 'Transaction_PIN',
                'OTP': 'Transaction_OTP',
                'Biometric': 'Transaction_Biometric'
            }
            auth_type = auth_type_mapping.get(auth_method, 'Transaction_PIN')
                
            auth_log = {
                'log_id': str(uuid.uuid4()),
                'customer_id': customer_id,
                'device_identifier': device_identifier,  
                'authentication_type': auth_type,        
                'transaction_id': transaction_record['transaction_id'],  
                'ip_address': ip_address,
                'status': status,                       
                'failure_reason': failure_reason,
                'otp_sent_to': customer['phone_number'] if 'OTP' in auth_method else None,
                'biometric_score': round(random.uniform(0.85, 0.99), 4) if 'Biometric' in auth_method else None,
                'attempt_count': 1,
                'session_id': str(uuid.uuid4())[:16],    # 16 char session ID
                'created_at': transaction_time
            }
                
            transaction_auth_logs.append(auth_log)
        
        # Progress indicator
        if (index + 1) % 100 == 0 or index == len(bank_account_df) - 1:
            progress = ((index + 1) / len(bank_account_df)) * 100
            print(f"Progress: {index + 1}/{len(bank_account_df)} ({progress:.1f}%)")
    
    transaction_df = pd.DataFrame(transactions)
    transaction_auth_log_df = pd.DataFrame(transaction_auth_logs)
    
    # Statistics
    total_transactions = len(transaction_df)
    completed_transactions = transaction_df[transaction_df['status'] == 'Completed'].shape[0]
    high_value_transactions = transaction_df[transaction_df['amount'] >= 10_000_000].shape[0]
    
    print(f"Successfully generated {total_transactions} transactions")
    print(f"Transaction statistics:")
    print(f"   - Total transactions: {total_transactions}")
    print(f"   - Completed: {completed_transactions} ({completed_transactions/total_transactions*100:.1f}%)")
    print(f"   - High-value (≥10M VND): {high_value_transactions} ({high_value_transactions/total_transactions*100:.1f}%)")
    print(f"   - Auth logs generated: {len(transaction_auth_log_df)}")
    
    return transaction_df, transaction_auth_log_df

def generate_customer_data():
    """
    Generate customer data for bank account opening use case
    
    Returns:
        pd.DataFrame: DataFrame with all customer records and columns
    """
    print("Starting customer data generation for bank account opening...")
    
    # Step 1: Get record count and set seed for reproducibility
    record_count = get_daily_seed()
    random.seed(get_time_based_seed())
    print(f"Generating {record_count} customer records...")
    
    # Step 2: Reset tracking for uniqueness constraints
    reset_phone_tracking()
    print("Reset phone number tracking for uniqueness...")
    
    # Step 3: Generate customers in batch with progress tracking
    customers = []
    
    for i in range(record_count):
        try:
            # Step 1: Basic info (independent)
            customer_id = str(uuid.uuid4())
            full_name = generate_full_name()
            gender = generate_gender(full_name)
            date_of_birth = generate_date_of_birth()
            age = calculate_age(date_of_birth)
            
            # Step 2: Contact info (phone unique)
            phone_number = generate_phone_number()
            email = generate_email(full_name, phone_number)
            
            # Step 3: Identity docs (dependent on each other)
            id_number, doc_type = generate_id_passport_number() 
            issue_date = generate_issue_date(date_of_birth)
            expiry_date = generate_expiry_date(issue_date, doc_type, date_of_birth)
            issuing_authority = generate_issuing_authority(doc_type)
            is_resident = generate_is_resident(doc_type)
            tax_id = generate_tax_identification_number()
            
            # Step 4: Professional & Address (dependent chain)
            occupation = generate_occupation()
            position = generate_position(occupation, age)
            residential_address = generate_residential_address()
            work_address = generate_work_address(occupation, residential_address)
            contact_address = generate_contact_address(residential_address, work_address, age)
            
            # Step 5: Financial & Risk (depends on many factors)
            customer_type = generate_customer_type()
            province = extract_province(residential_address)
            monthly_income = generate_monthly_income(occupation, age, province, customer_type)
            
            # Step 6: Security (depends on personal info)
            pin_hash = generate_pin_hash(full_name, date_of_birth, phone_number)
            password_hash, password_last_changed = generate_password_hash(full_name, date_of_birth, phone_number)
            
            # Step 7: Risk assessment (depends on all above)
            customer_data_for_risk = {
                'age': age, 
                'occupation': occupation, 
                'document_type': doc_type,
                'is_resident': is_resident, 
                'phone_valid': is_phone_valid(phone_number),
                'has_email': email is not None, 
                'province': province,
                'tax_id_valid': is_tax_id_valid(tax_id), 
                'id_passport_valid': is_id_valid(id_number, doc_type)
            }
            risk_score, risk_rating = calculate_risk_score_and_rating(customer_data_for_risk)
            
            # Step 8: Fixed values
            sms_notification_enabled = True
            email_notification_enabled = True
            created_at = datetime.now()
            status = generate_status()
            
            # Step 9: Fields to be set later (NULL for now)
            last_login_at = None
            failed_login_attempts = 0
            account_locked_until = None
            kyc_completed_at = None
            updated_at = None
            
            # Build complete customer record
            customer = {
                'customer_id': customer_id,
                'full_name': full_name,
                'gender': gender,
                'date_of_birth': date_of_birth,
                'phone_number': phone_number,
                'email': email,
                'tax_identification_number': tax_id,
                'id_passport_number': id_number,
                'issue_date': issue_date,
                'expiry_date': expiry_date,
                'issuing_authority': issuing_authority,
                'is_resident': is_resident,
                'occupation': occupation,
                'position': position,
                'work_address': work_address,
                'residential_address': residential_address,
                'contact_address': contact_address,
                'pin': pin_hash,        # Schema field is 'pin', not 'pin_hash'
                'password': password_hash,  # Schema field is 'password', not 'password_hash'
                'password_last_changed': password_last_changed,
                'risk_rating': risk_rating,
                'risk_score': risk_score,
                'customer_type': customer_type,
                'monthly_income': monthly_income,
                'sms_notification_enabled': sms_notification_enabled,
                'email_notification_enabled': email_notification_enabled,
                'created_at': created_at,
                'last_login_at': last_login_at,
                'failed_login_attempts': failed_login_attempts,
                'account_locked_until': account_locked_until,
                'kyc_completed_at': kyc_completed_at,
                'updated_at': updated_at,
                'status': status
            }
            
            customers.append(customer)
            
            # Progress indicator
            if (i + 1) % 100 == 0 or i == record_count - 1:
                progress = ((i + 1) / record_count) * 100
                print(f"Progress: {i + 1}/{record_count} ({progress:.1f}%)")
                
        except Exception as e:
            print(f"Error generating customer {i + 1}: {e}")
            # Continue with next customer rather than failing entire batch
            continue
    
    # Step 4: Convert to DataFrame
    df = pd.DataFrame(customers)
    
    print(f"Successfully generated {len(df)} customer records")
    print(f"DataFrame shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    return df

def generate_data():
    """
    Generate complete banking data pipeline
    
    Returns:
        dict: Dictionary containing all generated DataFrames:
            - customers: Customer DataFrame
            - devices: Device DataFrame 
            - auth_logs: Authentication log DataFrame (login)
            - bank_accounts: Bank account DataFrame
            - face_templates: Face template DataFrame
            - transactions: Transaction DataFrame
            - transaction_auth_logs: Transaction authentication log DataFrame
    """
    print("=" * 80)
    print("COMPLETE BANKING DATA PIPELINE")
    print("=" * 80)
    
    # Step 1: Generate customer data
    print("\n")
    print("STEP 1: CUSTOMER DATA GENERATION")
    print("-" * 50)
    customer_df = generate_customer_data()
    print(f"\nGenerated: {len(customer_df)} customers")
    
    # Step 2: Generate customer devices
    print("\n")
    print("STEP 2: CUSTOMER DEVICE DATA GENERATION")
    print("-" * 50)
    device_df = generate_customer_device_data(customer_df)
    print(f"\nGenerated: {len(device_df)} devices")
    
    # Step 3: Generate authentication logs (initial device authentication)
    print("\n")
    print("STEP 3: AUTHENTICATION LOG DATA GENERATION (Device Login)")
    print("-" * 50)
    auth_log_df = generate_authentication_log_data(customer_df, device_df)
    print(f"\nGenerated: {len(auth_log_df)} authentication logs")
    
    # Step 4: Generate bank accounts (after device verification)
    print("\n")
    print("STEP 4: BANK ACCOUNT DATA GENERATION")
    print("-" * 50)
    bank_account_df = generate_bank_account_data(customer_df, device_df)
    print(f"\nGenerated: {len(bank_account_df)} bank accounts")
    
    # Step 5: Generate face templates (KYC completion)
    print("\n")
    print("STEP 5: FACE TEMPLATE DATA GENERATION (KYC)")
    print("-" * 50)
    face_template_df, updated_customer_df = generate_face_template_data(customer_df)
    print(f"\nGenerated: {len(face_template_df)} face templates")
    
    # Step 6: Generate transactions and transaction authentication logs
    print("\n")
    print("STEP 6: TRANSACTION DATA GENERATION")
    print("-" * 50)
    transaction_df, transaction_auth_log_df = generate_transaction_data(
        updated_customer_df, bank_account_df, device_df, face_template_df
    )
    print(f"\nGenerated: {len(transaction_df)} transactions")
    print(f"Generated: {len(transaction_auth_log_df)} transaction auth logs")
    
    # Merge authentication logs into single DataFrame (schema has only 1 authentication_log table)
    print("\nMerging authentication logs...")
    combined_auth_logs = pd.concat([auth_log_df, transaction_auth_log_df], ignore_index=True)
    print(f"Combined authentication logs: {len(combined_auth_logs)} total records")
    
    # Final Summary
    print("\n")
    print("=" * 80)
    print("FINAL DATA GENERATION SUMMARY")
    print("=" * 80)
    print(f"Customer: {len(updated_customer_df):,}")
    print(f"Customer Device: {len(device_df):,}")
    print(f"Authentication Log (Login): {len(auth_log_df):,}")
    print(f"Bank Account: {len(bank_account_df):,}")
    print(f"Face Template: {len(face_template_df):,}")
    print(f"Transaction: {len(transaction_df):,}")
    print(f"Authentication Log (Transaction): {len(transaction_auth_log_df):,}")
    print(f"Authentication Log (Combined): {len(combined_auth_logs):,}")
    print("-" * 80)
    total_records = len(combined_auth_logs) + len(updated_customer_df) + len(device_df) + len(bank_account_df) + len(face_template_df) + len(transaction_df)
    print(f"TOTAL RECORDS: {total_records:,}")
    print("=" * 80)
    
    # Return all DataFrames matching exact database schema table names
    return {
        'customer': updated_customer_df,
        'customer_device': device_df,
        'authentication_log': combined_auth_logs,
        'bank_account': bank_account_df,
        'face_template': face_template_df,
        'transaction': transaction_df
    }

if __name__ == "__main__":
    # Generate all data using the new generate_data function
    data = generate_data()
    
    # Extract DataFrames for sample display
    customer_df = data['customer']
    device_df = data['customer_device']
    auth_log_df = data['authentication_log'] 
    bank_account_df = data['bank_account']
    face_template_df = data['face_template']
    transaction_df = data['transaction']
    
    # Display sample data for each table
    print("\nSample Customer Data (First 10 records):")
    print("=" * 120)
    sample_cols = ['customer_id', 'full_name', 'gender', 'phone_number', 'occupation', 'monthly_income', 'risk_rating']
    print(customer_df[sample_cols].head(10).to_string(index=False))
    print("=" * 120)
    
    print("\nSample Customer Device Data (First 10 records):")
    print("=" * 120)
    device_cols = ['device_identifier', 'customer_id', 'device_type', 'device_name', 'is_trusted', 'status']
    print(device_df[device_cols].head(10).to_string(index=False))
    print("=" * 120)
    
    print("\nSample Authentication Log Data (First 10 records):")
    print("=" * 120)
    auth_cols = ['log_id', 'customer_id', 'device_identifier', 'authentication_type', 'status', 'created_at']
    print(auth_log_df[auth_cols].head(10).to_string(index=False))
    print("=" * 120)
    
    print("\nSample Bank Account Data (First 10 records):")
    print("=" * 120)
    account_cols = ['account_id', 'customer_id', 'account_number', 'account_type', 'current_balance', 'is_primary', 'status']
    print(bank_account_df[account_cols].head(10).to_string(index=False))
    print("=" * 120)
    
    print("\nSample Face Template Data (First 10 records):")
    print("=" * 120)
    face_cols = ['template_id', 'customer_id', 'created_at', 'last_used_at']
    print(face_template_df[face_cols].head(10).to_string(index=False))
    print("=" * 120)
    
    print("\nSample Transaction Data (First 10 records):")
    print("=" * 140)
    trans_cols = ['transaction_id', 'account_id', 'transaction_type', 'amount', 'note', 'status', 'created_at']
    print(transaction_df[trans_cols].head(10).to_string(index=False))
    print("=" * 140)
    
    # Data Quality Preview for Part 3
    print("\n")
    print("DATA QUALITY PREVIEW (for Part 3)")
    print("-" * 50)
    
    # High-value transactions requiring strong auth
    high_value_trans = transaction_df[transaction_df['amount'] >= 10_000_000]
    print(f"High-value transactions (>=10M VND): {len(high_value_trans)}")
    
    # Untrusted device summary (note: transactions don't directly link to devices in schema)
    untrusted_devices = data['customer_device'][data['customer_device']['is_trusted'] == False]['device_identifier'].tolist()
    print(f"Number of untrusted devices: {len(untrusted_devices)}")
    # Note: Transaction-device relationship tracked via authentication logs
    
    # Daily transaction analysis would be done in data quality scripts
    print(f"Transaction date range: {transaction_df['created_at'].min()} to {transaction_df['created_at'].max()}")
    
    print("\nData ready for Part 3: Data Quality Checks!")
    print("=" * 80)
