import pandas as pd
import re
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =====================================================
# DATA QUALITY RULES CONFIGURATION
# =====================================================

# Required fields that cannot be NULL
REQUIRED_FIELDS = {
    'customer': ['full_name', 'date_of_birth', 'phone_number', 'id_passport_number', 
                 'residential_address', 'pin', 'password'],
    'bank_account': ['customer_id', 'account_number', 'account_type', 'currency'],
    'transaction': ['account_id', 'transaction_type', 'amount', 'currency', 'authentication_method'],
    'customer_device': ['device_identifier', 'customer_id', 'device_type'],
    'face_template': ['customer_id', 'encrypted_face_encoding'],
    'authentication_log': ['customer_id', 'device_identifier', 'authentication_type', 'status']
}

# Unique fields that must be unique across table
UNIQUE_FIELDS = {
    'customer': ['phone_number', 'email', 'tax_identification_number', 'id_passport_number'],
    'bank_account': ['account_number'],
    'customer_device': ['device_identifier'],
    'face_template': ['customer_id'],
    'transaction': ['transaction_id'],
    'authentication_log': ['log_id']
}

# Foreign key relationships to validate
FOREIGN_KEY_RELATIONSHIPS = [
    ('face_template', 'customer_id', 'customer', 'customer_id'),
    ('bank_account', 'customer_id', 'customer', 'customer_id'), 
    ('customer_device', 'customer_id', 'customer', 'customer_id'),
    ('transaction', 'account_id', 'bank_account', 'account_id'),
    ('authentication_log', 'customer_id', 'customer', 'customer_id'),
    ('authentication_log', 'transaction_id', 'transaction', 'transaction_id'),
    ('authentication_log', 'device_identifier', 'customer_device', 'device_identifier')
]

# =====================================================
# REGEX PATTERNS FOR FORMAT VALIDATION
# =====================================================
REGEX_PATTERNS = {
    'cccd_number': r'^\d{12}$',  # 12 digits for CCCD
    'passport_number': r'^[A-Z]\d{7}$',  # 1 letter + 7 digits for passport
    'vietnamese_phone': r'^(09|08|07|05|03)\d{8}$',  # Vietnamese mobile numbers
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'bvbank_account': r'^280\d{15}$',  # BVBank format: 280 + 15 digits = 18 total
    'device_identifier': r'^(IMEI|MAC|UUID|ANDROID_ID):[A-Za-z0-9:-]+$'
}

# =====================================================
# DATA QUALITY CHECK FUNCTIONS
# =====================================================
def check_null_missing_values(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Check for null/missing values in required fields (with row-level tracking)
    
    Args:
        data_dict: Dictionary of table_name -> DataFrame
        
    Returns:
        Dictionary containing null check results with failed row indices
    """
    logger.info("Starting null/missing values check...")
    
    results = {
        'check_type': 'null_missing_values',
        'status': 'PASS',
        'issues': [],
        'summary': {},
        'failed_rows': {}  # NEW: Track failed row indices by table
    }
    
    for table_name, df in data_dict.items():
        if table_name not in REQUIRED_FIELDS:
            continue
            
        table_issues = []
        failed_indices = set()  # Track all failed row indices for this table
        required_fields = REQUIRED_FIELDS[table_name]
        
        for field in required_fields:
            if field not in df.columns:
                continue
                
            # Find rows with null values in this required field
            null_mask = df[field].isnull()
            null_indices = df[null_mask].index.tolist()
            null_count = len(null_indices)
            total_count = len(df)
            null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
            
            if null_count > 0:
                # Add to failed indices
                failed_indices.update(null_indices)
                
                issue = {
                    'table': table_name,
                    'field': field,
                    'null_count': int(null_count),
                    'total_count': int(total_count),
                    'null_percentage': round(null_percentage, 2),
                    'failed_row_indices': null_indices  # Track specific failed rows
                }
                table_issues.append(issue)
                results['status'] = 'FAIL'
        
        if table_issues:
            results['issues'].extend(table_issues)
            
        # Store all failed row indices for this table
        if failed_indices:
            results['failed_rows'][table_name] = list(failed_indices)
        
        results['summary'][table_name] = {
            'total_records': len(df),
            'issues_count': len(table_issues),
            'failed_records': len(failed_indices)
        }
    
    logger.info(f"Null check completed: {results['status']}")
    return results

def check_uniqueness_constraints_with_database(data_dict: Dict[str, pd.DataFrame], 
                                               check_database: bool = True) -> Dict[str, Any]:
    """
    Check uniqueness constraints against both new data AND existing database data
    
    Args:
        data_dict: Dictionary of table_name -> DataFrame
        check_database: Whether to check against existing database data
        
    Returns:
        Dictionary containing uniqueness check results with failed row indices
    """
    logger.info("Starting uniqueness constraints check (with database comparison)...")
    
    results = {
        'check_type': 'uniqueness_constraints',
        'status': 'PASS',
        'issues': [],
        'summary': {},
        'failed_rows': {}  # Track failed row indices by table
    }
    
    # Try to get database connection for existing data check
    database_engine = None
    if check_database:
        try:
            from monitoring_audit import create_database_connection
            database_engine = create_database_connection()
            if database_engine:
                logger.info("Database connection established for uniqueness check")
            else:
                logger.warning("Database not available - checking only within new data")
        except Exception as e:
            logger.warning(f"Database connection failed: {str(e)} - checking only within new data")
    
    for table_name, df in data_dict.items():
        if table_name not in UNIQUE_FIELDS:
            continue
            
        table_issues = []
        failed_indices = set()  # Track all failed row indices for this table
        unique_fields = UNIQUE_FIELDS[table_name]
        
        for field in unique_fields:
            if field not in df.columns:
                continue
                
            # Step 1: Find duplicates within new data
            non_null_mask = df[field].notna()
            non_null_df = df[non_null_mask]
            
            internal_duplicated_mask = non_null_df[field].duplicated(keep=False)
            internal_duplicate_indices = non_null_df[internal_duplicated_mask].index.tolist()
            
            # Step 2: Check against existing database data (if available)
            database_conflicts = []
            if database_engine and check_database:
                try:
                    # Query existing values from database
                    query = f"SELECT DISTINCT {field} FROM {table_name} WHERE {field} IS NOT NULL"
                    existing_values = pd.read_sql(query, database_engine)
                    existing_set = set(existing_values[field].dropna())
                    
                    # Find new data that conflicts with existing data
                    for idx, value in zip(non_null_df.index, non_null_df[field]):
                        if value in existing_set:
                            database_conflicts.append(idx)
                    
                    if database_conflicts:
                        logger.info(f"Found {len(database_conflicts)} database conflicts for {table_name}.{field}")
                        
                except Exception as e:
                    logger.warning(f"Database uniqueness check failed for {table_name}.{field}: {str(e)}")
            
            # Combine all uniqueness violations
            all_failed_indices = list(set(internal_duplicate_indices + database_conflicts))
            
            if all_failed_indices:
                # Add to failed indices
                failed_indices.update(all_failed_indices)
                
                # Get sample duplicate values
                sample_values = non_null_df.loc[all_failed_indices[:5]][field].unique()
                
                issue = {
                    'table': table_name,
                    'field': field,
                    'total_non_null': len(non_null_df),
                    'unique_count': non_null_df[field].nunique(),
                    'duplicate_count': len(all_failed_indices),
                    'internal_duplicates': len(internal_duplicate_indices),
                    'database_conflicts': len(database_conflicts),
                    'sample_duplicates': list(sample_values),
                    'failed_row_indices': all_failed_indices,
                    'database_check_performed': database_engine is not None and check_database
                }
                table_issues.append(issue)
                results['status'] = 'FAIL'
        
        if table_issues:
            results['issues'].extend(table_issues)
            
        # Store all failed row indices for this table
        if failed_indices:
            results['failed_rows'][table_name] = list(failed_indices)
            
        results['summary'][table_name] = {
            'total_records': len(df),
            'issues_count': len(table_issues),
            'failed_records': len(failed_indices),
            'database_check_available': database_engine is not None and check_database
        }
    
    logger.info(f"Enhanced uniqueness check completed: {results['status']}")
    return results

def check_format_validation(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Check format/length validation using regex patterns (with row-level tracking)
    
    Args:
        data_dict: Dictionary of table_name -> DataFrame
        
    Returns:
        Dictionary containing format validation results with failed row indices
    """
    logger.info("Starting format/length validation check...")
    
    results = {
        'check_type': 'format_validation',
        'status': 'PASS',
        'issues': [],
        'summary': {},
        'failed_rows': {}  # NEW: Track failed row indices by table
    }
    
    # Field to regex pattern mapping
    field_patterns = {
        ('customer', 'id_passport_number'): ['cccd_number', 'passport_number'],  # Can be either
        ('customer', 'phone_number'): ['vietnamese_phone'],
        ('customer', 'email'): ['email'],
        ('bank_account', 'account_number'): ['bvbank_account'],
        ('customer_device', 'device_identifier'): ['device_identifier']
    }
    
    for table_name, df in data_dict.items():
        table_issues = []
        failed_indices = set()  # Track all failed row indices for this table
        
        for (table, field), pattern_names in field_patterns.items():
            if table != table_name or field not in df.columns:
                continue
                
            # Get non-null values with their indices
            non_null_mask = df[field].notna()
            non_null_df = df[non_null_mask]
            invalid_indices = []
            sample_invalid = []
            
            for idx, value in zip(non_null_df.index, non_null_df[field].astype(str)):
                is_valid = False
                # Check if value matches any of the allowed patterns
                for pattern_name in pattern_names:
                    if pattern_name in REGEX_PATTERNS:
                        pattern = REGEX_PATTERNS[pattern_name]
                        if re.match(pattern, value):
                            is_valid = True
                            break
                
                if not is_valid:
                    invalid_indices.append(idx)
                    if len(sample_invalid) < 5:
                        sample_invalid.append(value)
            
            if invalid_indices:
                # Add to failed indices
                failed_indices.update(invalid_indices)
                
                total_count = len(non_null_df)
                invalid_count = len(invalid_indices)
                invalid_percentage = (invalid_count / total_count) * 100 if total_count > 0 else 0
                
                issue = {
                    'table': table_name,
                    'field': field,
                    'invalid_count': invalid_count,
                    'total_count': total_count,
                    'invalid_percentage': round(invalid_percentage, 2),
                    'expected_patterns': pattern_names,
                    'sample_invalid': sample_invalid,
                    'failed_row_indices': invalid_indices  # Track specific failed rows
                }
                table_issues.append(issue)
                results['status'] = 'FAIL'
        
        if table_issues:
            results['issues'].extend(table_issues)
            
        # Store all failed row indices for this table
        if failed_indices:
            results['failed_rows'][table_name] = list(failed_indices)
            
        results['summary'][table_name] = {
            'total_records': len(df),
            'issues_count': len(table_issues),
            'failed_records': len(failed_indices)
        }
    
    logger.info(f"Format validation completed: {results['status']}")
    return results

def check_foreign_key_integrity(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Check foreign key integrity constraints (with row-level tracking)
    
    Args:
        data_dict: Dictionary of table_name -> DataFrame
        
    Returns:
        Dictionary containing foreign key check results with failed row indices
    """
    logger.info("Starting foreign key integrity check...")
    
    results = {
        'check_type': 'foreign_key_integrity',
        'status': 'PASS',
        'issues': [],
        'summary': {},
        'failed_rows': {}  # NEW: Track failed row indices by table
    }
    
    for child_table, child_field, parent_table, parent_field in FOREIGN_KEY_RELATIONSHIPS:
        if child_table not in data_dict or parent_table not in data_dict:
            continue
            
        child_df = data_dict[child_table]
        parent_df = data_dict[parent_table]
        
        if child_field not in child_df.columns or parent_field not in parent_df.columns:
            continue
        
        # Get non-null foreign key values with indices
        child_non_null_mask = child_df[child_field].notna()
        child_non_null_df = child_df[child_non_null_mask]
        parent_values = set(parent_df[parent_field].dropna())
        
        # Find orphaned records with their indices
        orphaned_mask = ~child_non_null_df[child_field].isin(parent_values)
        orphaned_indices = child_non_null_df[orphaned_mask].index.tolist()
        orphaned_values = child_non_null_df[orphaned_mask][child_field]
        
        if orphaned_indices:
            # Add to failed rows for this child table
            if child_table not in results['failed_rows']:
                results['failed_rows'][child_table] = []
            results['failed_rows'][child_table].extend(orphaned_indices)
            
            issue = {
                'child_table': child_table,
                'child_field': child_field,
                'parent_table': parent_table,
                'parent_field': parent_field,
                'orphaned_count': len(orphaned_indices),
                'total_child_records': len(child_non_null_df),
                'sample_orphaned': list(orphaned_values.unique())[:5],
                'failed_row_indices': orphaned_indices  # Track specific failed rows
            }
            results['issues'].append(issue)
            results['status'] = 'FAIL'
    
    # Remove duplicates from failed_rows
    for table_name in results['failed_rows']:
        results['failed_rows'][table_name] = list(set(results['failed_rows'][table_name]))
    
    results['summary'] = {
        'total_relationships_checked': len(FOREIGN_KEY_RELATIONSHIPS),
        'failed_relationships': len(results['issues']),
        'total_failed_records': sum(len(indices) for indices in results['failed_rows'].values())
    }
    
    logger.info(f"Foreign key check completed: {results['status']}")
    return results

# =====================================================
# RISK-BASED CHECK FUNCTIONS  
# =====================================================
def check_high_value_transaction_auth(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Check: Transactions more than 10M VND must use strong auth (biometric or OTP) - with row-level tracking
    UPDATED: Uses new authentication_method field and schema types
    
    Args:
        data_dict: Dictionary of table_name -> DataFrame
        
    Returns:
        Dictionary containing high value transaction auth check results with failed row indices
    """
    logger.info("Starting high value transaction authentication check...")
    
    results = {
        'check_type': 'high_value_transaction_auth',
        'status': 'PASS',
        'issues': [],
        'summary': {},
        'failed_rows': {}  # NEW: Track failed row indices by table
    }
    
    if 'transaction' not in data_dict:
        results['status'] = 'SKIP'
        results['summary'] = {'reason': 'No transaction data available'}
        return results
    
    transaction_df = data_dict['transaction']
    
    # Define high value threshold (10M VND)
    HIGH_VALUE_THRESHOLD = Decimal('10000000.00')
    
    # Define strong authentication methods - UPDATED FOR NEW SCHEMA
    STRONG_AUTH_METHODS = ['PIN_OTP', 'PIN_OTP_Biometric']
    
    # Filter high value transactions - only for transfers/payments, not all transaction types
    high_value_mask = (
        (transaction_df['amount'] >= float(HIGH_VALUE_THRESHOLD)) &
        (transaction_df['currency'] == 'VND') &
        (transaction_df['transaction_type'].isin(['Internal_Transfer', 'External_Transfer', 'Bill_Payment']))
    )
    high_value_transactions = transaction_df[high_value_mask]
    
    # Find violations (high value transactions without strong auth)
    violation_mask = ~high_value_transactions['authentication_method'].isin(STRONG_AUTH_METHODS)
    violations = high_value_transactions[violation_mask]
    violation_indices = violations.index.tolist()
    
    if violation_indices:
        results['status'] = 'FAIL'
        
        # Track failed row indices
        results['failed_rows']['transaction'] = violation_indices
        
        # Group violations by authentication method and transaction type
        violation_summary = violations.groupby(['authentication_method', 'transaction_type']).agg({
            'transaction_id': 'count',
            'amount': ['sum', 'mean', 'max']
        }).round(2)
        
        issue = {
            'rule': 'Transactions >= 10M VND must use strong authentication (PIN_OTP or PIN_OTP_Biometric)',
            'violation_count': len(violation_indices),
            'total_high_value_transactions': len(high_value_transactions),
            'violation_percentage': round((len(violation_indices) / len(high_value_transactions)) * 100, 2) if len(high_value_transactions) > 0 else 0,
            'violation_by_method_and_type': violation_summary.to_dict() if not violation_summary.empty else {},
            'sample_violations': violations[['transaction_id', 'transaction_type', 'amount', 'authentication_method']].head(5).to_dict('records'),
            'failed_row_indices': violation_indices  # Track specific failed rows
        }
        results['issues'].append(issue)
    
    results['summary'] = {
        'total_transactions': len(transaction_df),
        'high_value_transactions': len(high_value_transactions),
        'violations': len(violation_indices),
        'compliance_rate': round((1 - len(violation_indices) / len(high_value_transactions)) * 100, 2) if len(high_value_transactions) > 0 else 100
    }
    
    logger.info(f"High value transaction auth check completed: {results['status']}")
    return results

def check_device_verification_requirement(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Check: Device must be verified if new or untrusted
    UPDATED: Uses schema field names
    
    Args:
        data_dict: Dictionary of table_name -> DataFrame
        
    Returns:
        Dictionary containing device verification check results
    """
    logger.info("Starting device verification requirement check...")
    
    results = {
        'check_type': 'device_verification_requirement',
        'status': 'PASS',
        'issues': [],
        'summary': {},
        'failed_rows': {}  # NEW: Track failed row indices by table
    }
    
    if 'customer_device' not in data_dict:
        results['status'] = 'SKIP'
        results['summary'] = {'reason': 'No customer device data available'}
        return results
    
    device_df = data_dict['customer_device']
    
    # Find untrusted devices that are still active
    untrusted_mask = (
        (device_df['is_trusted'] == False) &
        (device_df['status'] == 'Active')
    )
    untrusted_devices = device_df[untrusted_mask]
    violation_indices = untrusted_devices.index.tolist()
    
    if violation_indices:
        results['status'] = 'FAIL'
        
        # Track failed row indices
        results['failed_rows']['customer_device'] = violation_indices
        
        # Group by device type
        device_type_summary = untrusted_devices.groupby('device_type').size().to_dict()
        
        issue = {
            'rule': 'Active devices must be trusted/verified',
            'untrusted_active_devices': len(violation_indices),
            'total_active_devices': len(device_df[device_df['status'] == 'Active']),
            'violation_percentage': round((len(violation_indices) / len(device_df)) * 100, 2) if len(device_df) > 0 else 0,
            'untrusted_by_device_type': device_type_summary,
            'sample_violations': untrusted_devices[['device_identifier', 'device_type', 'customer_id']].head(5).to_dict('records'),
            'failed_row_indices': violation_indices  # Track specific failed rows
        }
        results['issues'].append(issue)
    
    results['summary'] = {
        'total_devices': len(device_df),
        'active_devices': len(device_df[device_df['status'] == 'Active']),
        'trusted_devices': len(device_df[device_df['is_trusted'] == True]),
        'untrusted_active_devices': len(violation_indices),
        'trust_rate': round((len(device_df[device_df['is_trusted'] == True]) / len(device_df)) * 100, 2) if len(device_df) > 0 else 0
    }
    
    logger.info(f"Device verification check completed: {results['status']}")
    return results

def check_daily_transaction_limit_auth(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Check: Total transaction amount per customer > 20M VND in a day must have at least one strong auth method
    UPDATED: Uses new authentication_method field and transaction types
    
    Args:
        data_dict: Dictionary of table_name -> DataFrame
        
    Returns:
        Dictionary containing daily transaction limit auth check results
    """
    logger.info("Starting daily transaction limit authentication check...")
    
    results = {
        'check_type': 'daily_transaction_limit_auth',
        'status': 'PASS',
        'issues': [],
        'summary': {},
        'failed_rows': {}  # NEW: Track failed row indices by table
    }
    
    if 'transaction' not in data_dict or 'bank_account' not in data_dict:
        results['status'] = 'SKIP'
        results['summary'] = {'reason': 'Missing transaction or bank_account data'}
        return results
    
    transaction_df = data_dict['transaction']
    account_df = data_dict['bank_account']
    
    # Join transactions with accounts to get customer_id
    transaction_with_customer = transaction_df.merge(
        account_df[['account_id', 'customer_id']], 
        on='account_id', 
        how='left'
    )
    
    # Filter VND transactions and completed ones - only count transfer/payment types
    vnd_transactions = transaction_with_customer[
        (transaction_with_customer['currency'] == 'VND') &
        (transaction_with_customer['status'] == 'Completed') &
        (transaction_with_customer['transaction_type'].isin(['Internal_Transfer', 'External_Transfer', 'Bill_Payment']))
    ].copy()  # Create explicit copy to avoid SettingWithCopyWarning
    
    # Group by customer and date to calculate daily totals
    vnd_transactions['transaction_date'] = pd.to_datetime(vnd_transactions['created_at']).dt.date
    
    daily_customer_summary = vnd_transactions.groupby(['customer_id', 'transaction_date']).agg({
        'amount': 'sum',
        'authentication_method': lambda x: list(x.dropna()),
        'transaction_id': 'count'
    }).reset_index()
    
    # Define daily limit threshold (20M VND)
    DAILY_LIMIT_THRESHOLD = Decimal('20000000.00')
    STRONG_AUTH_METHODS = ['PIN_OTP', 'PIN_OTP_Biometric']  # UPDATED for new schema
    
    # Find customers exceeding daily limit
    high_daily_transactions = daily_customer_summary[
        daily_customer_summary['amount'] >= float(DAILY_LIMIT_THRESHOLD)
    ]
    
    # Check if they have at least one strong auth method
    violations = []
    for _, row in high_daily_transactions.iterrows():
        auth_methods = row['authentication_method']
        has_strong_auth = any(method in STRONG_AUTH_METHODS for method in auth_methods if method)
        
        if not has_strong_auth:
            violations.append({
                'customer_id': row['customer_id'],
                'transaction_date': str(row['transaction_date']),
                'total_amount': float(row['amount']),
                'transaction_count': int(row['transaction_id']),
                'auth_methods_used': list(set(auth_methods))
            })
    
    violation_count = len(violations)
    
    if violation_count > 0:
        results['status'] = 'FAIL'
        
        issue = {
            'rule': 'Daily transactions >= 20M VND must have at least one strong authentication (PIN_OTP or PIN_OTP_Biometric)',
            'violation_count': violation_count,
            'total_high_daily_volume_cases': len(high_daily_transactions),
            'violation_percentage': round((violation_count / len(high_daily_transactions)) * 100, 2) if len(high_daily_transactions) > 0 else 0,
            'sample_violations': violations[:5]
        }
        results['issues'].append(issue)
    
    results['summary'] = {
        'total_customer_days_analyzed': len(daily_customer_summary),
        'high_volume_days': len(high_daily_transactions),
        'violation_days': violation_count,
        'compliance_rate': round((1 - violation_count / len(high_daily_transactions)) * 100, 2) if len(high_daily_transactions) > 0 else 100
    }
    
    logger.info(f"Daily transaction limit auth check completed: {results['status']}")
    return results

def check_transaction_type_constraints(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Validate transaction type specific constraints based on schema
    
    Args:
        data_dict: Dictionary of table_name -> DataFrame
        
    Returns:
        Dictionary containing transaction type constraint check results
    """
    logger.info("Starting transaction type constraints check...")
    
    results = {
        'check_type': 'transaction_type_constraints',
        'status': 'PASS',
        'issues': [],
        'summary': {},
        'failed_rows': {}  # Track failed row indices by table
    }
    
    if 'transaction' not in data_dict:
        results['status'] = 'SKIP'
        results['summary'] = {'reason': 'No transaction data available'}
        return results
    
    transaction_df = data_dict['transaction']
    
    violations = []
    
    for _, row in transaction_df.iterrows():
        transaction_type = row['transaction_type']
        recipient_account = row.get('recipient_account_number')
        recipient_bank_code = row.get('recipient_bank_code')
        service_provider_code = row.get('service_provider_code')
        bill_number = row.get('bill_number')
        
        # Check Internal_Transfer constraints
        if transaction_type == 'Internal_Transfer':
            if pd.isna(recipient_account) or not pd.isna(recipient_bank_code):
                violations.append({
                    'transaction_id': row['transaction_id'],
                    'transaction_type': transaction_type,
                    'violation': 'Internal transfers must have recipient_account_number but recipient_bank_code must be NULL',
                    'recipient_account': recipient_account,
                    'recipient_bank_code': recipient_bank_code
                })
        
        # Check External_Transfer constraints  
        elif transaction_type == 'External_Transfer':
            if pd.isna(recipient_account) or pd.isna(recipient_bank_code):
                violations.append({
                    'transaction_id': row['transaction_id'],
                    'transaction_type': transaction_type,
                    'violation': 'External transfers must have both recipient_account_number and recipient_bank_code',
                    'recipient_account': recipient_account,
                    'recipient_bank_code': recipient_bank_code
                })
        
        # Check Bill_Payment constraints
        elif transaction_type == 'Bill_Payment':
            if not pd.isna(recipient_account) or not pd.isna(recipient_bank_code):
                violations.append({
                    'transaction_id': row['transaction_id'],
                    'transaction_type': transaction_type,
                    'violation': 'Bill payments must have recipient_account_number and recipient_bank_code as NULL',
                    'recipient_account': recipient_account,
                    'recipient_bank_code': recipient_bank_code
                })
            if pd.isna(service_provider_code) or pd.isna(bill_number):
                violations.append({
                    'transaction_id': row['transaction_id'],
                    'transaction_type': transaction_type,
                    'violation': 'Bill payments must have service_provider_code and bill_number',
                    'service_provider_code': service_provider_code,
                    'bill_number': bill_number
                })
    
    violation_count = len(violations)
    
    if violation_count > 0:
        results['status'] = 'FAIL'
        
        # Group violations by type
        violation_types = {}
        for v in violations:
            vtype = v['violation']
            if vtype not in violation_types:
                violation_types[vtype] = 0
            violation_types[vtype] += 1
        
        issue = {
            'rule': 'Transaction type specific field constraints must be satisfied',
            'violation_count': violation_count,
            'total_transactions': len(transaction_df),
            'violation_percentage': round((violation_count / len(transaction_df)) * 100, 2) if len(transaction_df) > 0 else 0,
            'violations_by_type': violation_types,
            'sample_violations': violations[:10]
        }
        results['issues'].append(issue)
    
    results['summary'] = {
        'total_transactions': len(transaction_df),
        'internal_transfers': len(transaction_df[transaction_df['transaction_type'] == 'Internal_Transfer']),
        'external_transfers': len(transaction_df[transaction_df['transaction_type'] == 'External_Transfer']),
        'bill_payments': len(transaction_df[transaction_df['transaction_type'] == 'Bill_Payment']),
        'constraint_violations': violation_count
    }
    
    logger.info(f"Transaction type constraints check completed: {results['status']}")
    return results

# =====================================================
# MAIN RUNNER FUNCTION
# =====================================================
def run_all_data_quality_checks(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Run all data quality and risk-based checks
    
    Args:
        data_dict: Dictionary of table_name -> DataFrame
        
    Returns:
        Dictionary containing all check results
    """
    logger.info("Starting comprehensive data quality audit...")
    
    # Define requirement descriptions for each check (matching original assignment requirements)
    requirement_descriptions = {
        'null_missing_values': 'Null/missing values - Required fields must not be empty',
        'uniqueness_constraints': 'Uniqueness - Primary keys and unique fields must be unique across table',
        'format_validation': 'Format/length validation - Data must match specified formats (phone numbers, emails, account numbers, etc.)',
        'foreign_key_integrity': 'Foreign key integrity - All foreign key references must exist in parent tables',
        'high_value_transaction_auth': 'Transactions more than 10M VND must use strong auth (biometric or OTP) - per 2345/QĐ-NHNN 2023',
        'device_verification_requirement': 'Device must be verified if new or untrusted - Active devices must be trusted/verified',
        'daily_transaction_limit_auth': 'Total transaction amount per customer more than 20M VND in a day must have at least one strong auth method - per 2345/QĐ-NHNN 2023',
        'transaction_type_constraints': 'Transaction type constraints - Transaction-specific field requirements must be satisfied based on transaction type'
    }
    
    # Run all checks
    checks = {
        'null_missing_values': check_null_missing_values(data_dict),
        'uniqueness_constraints': check_uniqueness_constraints_with_database(data_dict, check_database=True),
        'format_validation': check_format_validation(data_dict),
        'foreign_key_integrity': check_foreign_key_integrity(data_dict),
        'high_value_transaction_auth': check_high_value_transaction_auth(data_dict),
        'device_verification_requirement': check_device_verification_requirement(data_dict),
        'daily_transaction_limit_auth': check_daily_transaction_limit_auth(data_dict),
        'transaction_type_constraints': check_transaction_type_constraints(data_dict)
    }
    
    # Add requirement descriptions to each check result
    for check_name, check_result in checks.items():
        check_result['requirement'] = requirement_descriptions.get(check_name, f'Unknown requirement for {check_name}')
    
    # Calculate overall summary
    total_checks = len(checks)
    passed_checks = sum(1 for result in checks.values() if result['status'] == 'PASS')
    failed_checks = sum(1 for result in checks.values() if result['status'] == 'FAIL')
    skipped_checks = sum(1 for result in checks.values() if result['status'] == 'SKIP')
    
    overall_status = 'PASS' if failed_checks == 0 else 'FAIL'
    
    audit_summary = {
        'audit_timestamp': datetime.now().isoformat(),
        'overall_status': overall_status,
        'total_checks': total_checks,
        'passed_checks': passed_checks,
        'failed_checks': failed_checks,
        'skipped_checks': skipped_checks,
        'pass_rate': round((passed_checks / total_checks) * 100, 2),
        'data_tables_analyzed': list(data_dict.keys()),
        'total_records_analyzed': sum(len(df) for df in data_dict.values())
    }
    
    result = {
        'audit_summary': audit_summary,
        'check_results': checks
    }
    
    logger.info(f"Data quality audit completed: {overall_status} ({passed_checks}/{total_checks} checks passed)")
    return result

# =====================================================
# DATA CLEANING HELPER FUNCTIONS
# =====================================================
def aggregate_failed_rows(check_results: Dict[str, Any]) -> Dict[str, List[int]]:
    """
    Aggregate all failed row indices from various data quality checks
    
    Args:
        check_results: Results from run_all_data_quality_checks
        
    Returns:
        Dictionary mapping table_name -> list of failed row indices
    """
    logger.info("Aggregating failed rows from all quality checks...")
    
    failed_rows_by_table = {}
    
    for check_name, check_result in check_results.items():
        if 'failed_rows' in check_result and check_result['failed_rows']:
            for table_name, failed_indices in check_result['failed_rows'].items():
                if table_name not in failed_rows_by_table:
                    failed_rows_by_table[table_name] = set()
                failed_rows_by_table[table_name].update(failed_indices)
    
    # Convert sets back to lists and sort
    result = {
        table: sorted(list(indices)) 
        for table, indices in failed_rows_by_table.items()
    }
    
    total_failed = sum(len(indices) for indices in result.values())
    logger.info(f"Aggregated {total_failed} failed rows across {len(result)} tables")
    
    return result

def handle_foreign_key_dependencies(cleaned_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Handle foreign key dependencies after cleaning individual tables
    Remove child records whose parent records were deleted
    
    Args:
        cleaned_data: Dictionary of cleaned DataFrames
        
    Returns:
        Dictionary of DataFrames with foreign key dependencies resolved
    """
    logger.info("Handling foreign key dependencies after cleaning...")
    
    result_data = cleaned_data.copy()
    dependency_stats = {}
    
    # Get valid parent keys for each relationship
    for child_table, child_field, parent_table, parent_field in FOREIGN_KEY_RELATIONSHIPS:
        if child_table not in result_data or parent_table not in result_data:
            continue
            
        # Get valid parent keys
        if parent_field in result_data[parent_table].columns:
            valid_parent_keys = set(result_data[parent_table][parent_field].dropna())
        else:
            continue
        
        # Clean child table
        if child_field in result_data[child_table].columns:
            original_count = len(result_data[child_table])
            
            # Keep only child records with valid parent references
            valid_mask = (
                result_data[child_table][child_field].isna() | 
                result_data[child_table][child_field].isin(valid_parent_keys)
            )
            result_data[child_table] = result_data[child_table][valid_mask].reset_index(drop=True)
            
            removed_count = original_count - len(result_data[child_table])
            dependency_stats[f"{child_table}.{child_field} -> {parent_table}.{parent_field}"] = {
                'original_count': original_count,
                'removed_count': removed_count,
                'final_count': len(result_data[child_table])
            }
            
            if removed_count > 0:
                logger.info(f"Removed {removed_count} {child_table} records due to missing {parent_table} references")
    
    total_removed = sum(stat['removed_count'] for stat in dependency_stats.values())
    logger.info(f"Foreign key dependency handling completed: {total_removed} additional records removed")
    
    return result_data

def clean_dataframes_by_failed_rows(data_dict: Dict[str, pd.DataFrame], 
                                  failed_rows_by_table: Dict[str, List[int]]) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    """
    Remove failed rows from DataFrames and return cleaned data with summary
    
    Args:
        data_dict: Original data dictionary
        failed_rows_by_table: Dictionary mapping table_name -> failed row indices
        
    Returns:
        Tuple of (cleaned_data_dict, cleaning_summary)
    """
    logger.info("Cleaning DataFrames by removing failed rows...")
    
    cleaned_data = {}
    cleaning_summary = {}
    
    for table_name, df in data_dict.items():
        original_count = len(df)
        
        if table_name in failed_rows_by_table:
            failed_indices = failed_rows_by_table[table_name]
            # Remove failed rows and reset index
            cleaned_df = df.drop(index=failed_indices, errors='ignore').reset_index(drop=True)
            removed_count = len(failed_indices)
        else:
            # No failed rows for this table
            cleaned_df = df.copy()
            removed_count = 0
        
        cleaned_data[table_name] = cleaned_df
        
        cleaning_summary[table_name] = {
            'original_count': original_count,
            'removed_count': removed_count,
            'final_count': len(cleaned_df),
            'cleaned_percentage': round((len(cleaned_df) / original_count) * 100, 2) if original_count > 0 else 0
        }
        
        if removed_count > 0:
            logger.info(f"{table_name}: {len(cleaned_df)}/{original_count} "
                       f"({cleaning_summary[table_name]['cleaned_percentage']}%) clean rows")
    
    # Handle foreign key dependencies after individual table cleaning
    cleaned_data = handle_foreign_key_dependencies(cleaned_data)
    
    # Update final counts after dependency handling
    for table_name in cleaning_summary:
        if table_name in cleaned_data:
            final_count_after_fk = len(cleaned_data[table_name])
            additional_removed = cleaning_summary[table_name]['final_count'] - final_count_after_fk
            
            cleaning_summary[table_name]['final_count_after_fk_cleanup'] = final_count_after_fk
            cleaning_summary[table_name]['total_removed'] = cleaning_summary[table_name]['removed_count'] + additional_removed
            cleaning_summary[table_name]['final_cleaned_percentage'] = round(
                (final_count_after_fk / cleaning_summary[table_name]['original_count']) * 100, 2
            ) if cleaning_summary[table_name]['original_count'] > 0 else 0
    
    total_original = sum(summary['original_count'] for summary in cleaning_summary.values())
    total_final = sum(len(df) for df in cleaned_data.values())
    overall_clean_percentage = round((total_final / total_original) * 100, 2) if total_original > 0 else 0
    
    logger.info(f"Data cleaning completed: {total_final}/{total_original} ({overall_clean_percentage}%) records retained")
    
    return cleaned_data, cleaning_summary

def run_comprehensive_data_cleaning(data_dict: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any], Dict[str, Any]]:
    """
    Run complete data quality audit and cleaning pipeline
    
    Args:
        data_dict: Original data dictionary
        
    Returns:
        Tuple of (cleaned_data, audit_results, cleaning_summary)
    """
    logger.info("Starting comprehensive data quality audit and cleaning...")
    
    # Step 1: Run all quality checks
    audit_results = run_all_data_quality_checks(data_dict)
    
    # Step 2: Aggregate failed rows
    failed_rows_by_table = aggregate_failed_rows(audit_results['check_results'])
    
    # Step 3: Clean data by removing failed rows
    cleaned_data, cleaning_summary = clean_dataframes_by_failed_rows(data_dict, failed_rows_by_table)
    
    # Step 4: Add cleaning info to audit results
    audit_results['cleaning_summary'] = cleaning_summary
    audit_results['failed_rows_aggregated'] = failed_rows_by_table
    
    return cleaned_data, audit_results, cleaning_summary
