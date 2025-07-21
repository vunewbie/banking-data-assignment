"""
BANKING DATA MONITORING & AUDIT SYSTEM
======================================

This module handles in-memory data quality audits and database operations 
for the banking system.

Features:
- Work with in-memory DataFrames from generate_data.py
- Comprehensive audit execution using data_quality_standards
- Detailed logging and summary table generation  
- Database insertion ONLY for clean data that passes quality checks
- Report export to logs/ and reports/ directories
"""

import os
import sys
import pandas as pd
import psycopg2
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
import logging
from sqlalchemy import create_engine
import traceback

# Add src to Python path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_quality_standards import run_comprehensive_data_cleaning

# =====================================================
# HELPER FUNCTIONS
# =====================================================

def get_date_based_folder(base_dir: str, subfolder: str = "") -> str:
    """
    Create date-based folder structure (yyyy-mm-dd)
    
    Args:
        base_dir: Base directory (logs or reports)
        subfolder: Optional subfolder (e.g., 'scheduler' for logs)
        
    Returns:
        Path to date-based folder
    """
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    if subfolder:
        folder_path = os.path.join(base_dir, subfolder, date_str)
    else:
        folder_path = os.path.join(base_dir, date_str)
    
    # Create folder if it doesn't exist
    os.makedirs(folder_path, exist_ok=True)
    return folder_path

# =====================================================
# LOGGING CONFIGURATION
# =====================================================

def setup_logging() -> logging.Logger:
    """
    Configure logging for audit operations
    
    Returns:
        Configured logger instance
    """
    # Create date-based logs directory: logs/scheduler/yyyy-mm-dd/
    base_logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
    logs_dir = get_date_based_folder(base_logs_dir, 'scheduler')
    
    # Configure logging with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = os.path.join(logs_dir, f'banking_audit_{timestamp}.log')
    
    # Create formatter
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Configure logger
    logger = logging.getLogger('banking_audit')
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplication
    logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logger.info(f"Logging configured. Log file: {log_filename}")
    return logger

# =====================================================
# DATABASE OPERATIONS (FOR CLEAN DATA ONLY)
# =====================================================

def create_database_connection(host: str = None, port: int = None, 
                             database: str = None, user: str = None, 
                             password: str = None) -> Optional[object]:
    """
    Create database connection with environment-aware defaults
    
    Args:
        host: Database host (defaults to env BANKING_DB_HOST or 'localhost')
        port: Database port (defaults to env BANKING_DB_PORT or 5433)
        database: Database name (defaults to env BANKING_DB_NAME or 'banking_system')
        user: Database username (defaults to env BANKING_DB_USER or 'postgres')
        password: Database password (defaults to env BANKING_DB_PASSWORD or 'postgres')
        
    Returns:
        SQLAlchemy engine or None if connection fails
    """
    import os
    logger = logging.getLogger('banking_audit')
    
    # Use environment variables if parameters not provided
    host = host or os.getenv('BANKING_DB_HOST', 'localhost')
    port = port or int(os.getenv('BANKING_DB_PORT', '5433'))
    database = database or os.getenv('BANKING_DB_NAME', 'banking_system')
    user = user or os.getenv('BANKING_DB_USER', 'postgres')
    password = password or os.getenv('BANKING_DB_PASSWORD', 'postgres')
    
    logger.info(f"Connecting to database: {host}:{port}/{database} as {user}")
    
    try:
        # Create SQLAlchemy engine
        connection_string = (
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )
        
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            logger.info(f"Database connected successfully: {version}")
            
        return engine
        
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return None

def map_columns_to_database(df: pd.DataFrame, df_key: str) -> Optional[pd.DataFrame]:
    """
    Map DataFrame columns to match database schema
    
    Args:
        df: DataFrame to map
        df_key: Type of DataFrame (customers, devices, etc.)
        
    Returns:
        Mapped DataFrame or None if mapping fails
    """
    logger = logging.getLogger('banking_audit')
    
    try:
        # Define column mappings from generate_data.py to database schema
        column_mappings = {
            'customer': {
                'customer_id': 'customer_id',
                'full_name': 'full_name',
                'gender': 'gender',
                'date_of_birth': 'date_of_birth',
                'phone_number': 'phone_number',
                'email': 'email',
                'tax_identification_number': 'tax_identification_number',
                'id_passport_number': 'id_passport_number',
                'issue_date': 'issue_date',
                'expiry_date': 'expiry_date',
                'issuing_authority': 'issuing_authority',
                'is_resident': 'is_resident',
                'occupation': 'occupation',
                'position': 'position',
                'work_address': 'work_address',
                'residential_address': 'residential_address',
                'contact_address': 'contact_address',
                'pin': 'pin',
                'password': 'password',
                'risk_rating': 'risk_rating',
                'risk_score': 'risk_score',
                'customer_type': 'customer_type',
                'monthly_income': 'monthly_income',
                'created_at': 'created_at',
                'updated_at': 'updated_at',
                'status': 'status',
                'kyc_completed_at': 'kyc_completed_at',
                'last_login_at': 'last_login_at',
                'failed_login_attempts': 'failed_login_attempts',
                'account_locked_until': 'account_locked_until',
                'password_last_changed': 'password_last_changed',
                'sms_notification_enabled': 'sms_notification_enabled',
                'email_notification_enabled': 'email_notification_enabled'
            },
            'face_template': {
                'template_id': 'template_id',
                'customer_id': 'customer_id',
                'encrypted_face_encoding': 'encrypted_face_encoding',
                'created_at': 'created_at',
                'last_used_at': 'last_used_at'
            },
            'bank_account': {
                'account_id': 'account_id',
                'customer_id': 'customer_id',
                'account_number': 'account_number',
                'account_type': 'account_type',
                'currency': 'currency',
                'available_balance': 'available_balance',
                'current_balance': 'current_balance',
                'hold_amount': 'hold_amount',
                'daily_transfer_limit': 'daily_transfer_limit',
                'daily_online_payment_limit': 'daily_online_payment_limit',
                'is_primary': 'is_primary',
                'status': 'status',
                'is_online_payment_enabled': 'is_online_payment_enabled',
                'interest_rate': 'interest_rate',
                'last_transaction_at': 'last_transaction_at',
                'open_at': 'open_at',
                'updated_at': 'updated_at'
            },
            'customer_device': {
                'device_identifier': 'device_identifier',
                'customer_id': 'customer_id',
                'device_type': 'device_type',
                'device_name': 'device_name',
                'is_trusted': 'is_trusted',
                'status': 'status',
                'first_seen_at': 'first_seen_at',
                'last_used_at': 'last_used_at'
            },
            'transaction': {
                'transaction_id': 'transaction_id',
                'account_id': 'account_id',
                'transaction_type': 'transaction_type',
                'amount': 'amount',
                'currency': 'currency',
                'fee': 'fee',
                'status': 'status',
                'note': 'note',
                'authentication_method': 'authentication_method',
                'recipient_account_number': 'recipient_account_number',
                'recipient_bank_code': 'recipient_bank_code',
                'recipient_name': 'recipient_name',
                'service_provider_code': 'service_provider_code',
                'bill_number': 'bill_number',
                'is_fraud': 'is_fraud',
                'fraud_score': 'fraud_score',
                'created_at': 'created_at',
                'completed_at': 'completed_at'
            },
            'authentication_log': {
                'log_id': 'log_id',
                'customer_id': 'customer_id',
                'device_identifier': 'device_identifier',
                'authentication_type': 'authentication_type',
                'transaction_id': 'transaction_id',
                'ip_address': 'ip_address',
                'status': 'status',
                'failure_reason': 'failure_reason',
                'otp_sent_to': 'otp_sent_to',
                'biometric_score': 'biometric_score',
                'attempt_count': 'attempt_count',
                'session_id': 'session_id',
                'created_at': 'created_at'
            }
        }
        
        if df_key not in column_mappings:
            logger.error(f"No column mapping defined for {df_key}")
            return None
        
        mapping = column_mappings[df_key]
        df_mapped = df.copy()
        
        # Apply column mappings
        available_columns = {}
        for source_col, target_col in mapping.items():
            if source_col in df_mapped.columns:
                available_columns[target_col] = df_mapped[source_col]
            else:
                logger.debug(f"Column {source_col} not found in {df_key} DataFrame")
        
        # Special handling for authentication_log status mapping
        if df_key == 'authentication_log' and 'status' in df_mapped.columns:
            if df_mapped['status'].dtype == 'bool':
                available_columns['status'] = df_mapped['status'].map({True: 'Success', False: 'Failed'})
            else:
                available_columns['status'] = df_mapped['status']
        
        # Create new DataFrame with mapped columns
        result_df = pd.DataFrame(available_columns)
        
        logger.info(f"Mapped {len(df.columns)} -> {len(result_df.columns)} columns for {df_key}")
        return result_df
        
    except Exception as e:
        logger.error(f"Column mapping failed for {df_key}: {str(e)}")
        logger.error(f"Available columns: {list(df.columns)}")
        return None

def save_clean_data_to_database(clean_data_dict: Dict[str, pd.DataFrame]) -> bool:
    """
    Save clean data that passed quality checks to database
    
    Args:
        clean_data_dict: Dictionary of table_name -> clean DataFrame
        
    Returns:
        True if save successful, False otherwise
    """
    logger = logging.getLogger('banking_audit')
    engine = create_database_connection()
    
    if not engine:
        return False
        
    try:
        logger.info("Starting to save clean data to database...")
        
        # Define table insertion order (respecting foreign key dependencies)
        table_order = [
            ('customer', 'customer'),
            ('face_template', 'face_template'),
            ('bank_account', 'bank_account'),
            ('customer_device', 'customer_device'),
            ('transaction', 'transaction'),
            ('authentication_log', 'authentication_log')
        ]
        
        total_saved = 0
        
        for db_table_name, df_key in table_order:
            if df_key in clean_data_dict:
                df = clean_data_dict[df_key]
                
                # Map DataFrame columns to database columns
                df_mapped = map_columns_to_database(df, df_key)
                
                if df_mapped is not None:
                    # Save to database
                    rows_inserted = len(df_mapped)
                    df_mapped.to_sql(db_table_name, engine, if_exists='append', index=False)
                    
                    logger.info(f"Saved {rows_inserted} records to {db_table_name}")
                    total_saved += rows_inserted
                else:
                    logger.warning(f"Skipping {db_table_name} due to column mapping issues")
            else:
                logger.warning(f"No data found for {db_table_name}")
        
        logger.info(f"Database save completed: {total_saved} total records saved")
        return True
        
    except Exception as e:
        logger.error(f"Database save failed: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

# =====================================================
# REPORT GENERATION
# =====================================================

def generate_audit_log(audit_results: Dict[str, Any]) -> str:
    """
    Generate detailed audit log
    
    Args:
        audit_results: Results from data quality audit
        
    Returns:
        Path to generated log file
    """
    logger = logging.getLogger('banking_audit')
    
    # Create date-based reports directory: reports/yyyy-mm-dd/
    base_reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'reports')
    reports_dir = get_date_based_folder(base_reports_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(reports_dir, f'audit_detailed_log_{timestamp}.txt')
    
    try:
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("BANKING DATA QUALITY AUDIT - DETAILED LOG\n")
            f.write("=" * 80 + "\n")
            f.write(f"Audit Timestamp: {audit_results['audit_summary']['audit_timestamp']}\n")
            f.write(f"Overall Status: {audit_results['audit_summary']['overall_status']}\n")
            f.write(f"Pass Rate: {audit_results['audit_summary']['pass_rate']}%\n")
            f.write("\n")
            
            # Summary section
            f.write("AUDIT SUMMARY\n")
            f.write("-" * 40 + "\n")
            summary = audit_results['audit_summary']
            f.write(f"Total Checks: {summary['total_checks']}\n")
            f.write(f"Passed: {summary['passed_checks']}\n")
            f.write(f"Failed: {summary['failed_checks']}\n")
            f.write(f"Skipped: {summary['skipped_checks']}\n")
            f.write(f"Records Analyzed: {summary['total_records_analyzed']}\n")
            f.write(f"Tables: {', '.join(summary['data_tables_analyzed'])}\n")
            f.write("\n")
            
            # Detailed results for each check
            for check_name, check_result in audit_results['check_results'].items():
                f.write(f"CHECK: {check_name.upper()}\n")
                f.write("-" * 60 + "\n")
                f.write(f"Status: {check_result['status']}\n")
                f.write(f"Requirement: {check_result.get('requirement', 'No requirement specified')}\n")
                
                if 'summary' in check_result:
                    f.write("Summary: " + str(check_result['summary']) + "\n")
                
                if check_result['status'] == 'FAIL' and check_result['issues']:
                    f.write("\nISSUES FOUND:\n")
                    for i, issue in enumerate(check_result['issues'], 1):
                        f.write(f"  {i}. {json.dumps(issue, indent=4, default=str)}\n")
                
                f.write("\n" + "=" * 60 + "\n\n")
            
        logger.info(f"Detailed audit log generated: {log_file}")
        return log_file
        
    except Exception as e:
        logger.error(f"Failed to generate audit log: {str(e)}")
        return ""

def generate_summary_table(audit_results: Dict[str, Any]) -> str:
    """
    Generate executive summary table with requirement descriptions
    
    Args:
        audit_results: Results from data quality audit
        
    Returns:
        Path to generated summary CSV file
    """
    logger = logging.getLogger('banking_audit')
    
    # Create date-based reports directory: reports/yyyy-mm-dd/
    base_reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'reports')
    reports_dir = get_date_based_folder(base_reports_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    summary_file = os.path.join(reports_dir, f'audit_summary_table_{timestamp}.csv')
    
    try:
        # Create summary rows
        summary_data = []
        
        for check_name, check_result in audit_results['check_results'].items():
            row = {
                'check_name': check_name,
                'requirement': check_result.get('requirement', f'Unknown requirement for {check_name}'),
                'status': check_result['status'],
                'issues_count': len(check_result.get('issues', [])),
                'check_type': check_result.get('check_type', ''),
                'timestamp': audit_results['audit_summary']['audit_timestamp']
            }
            
            # Add specific metrics based on check type
            if check_result['status'] == 'FAIL' and check_result.get('issues'):
                first_issue = check_result['issues'][0]
                if 'violation_count' in first_issue:
                    row['violation_count'] = first_issue['violation_count']
                if 'compliance_rate' in first_issue:
                    row['compliance_rate'] = first_issue['compliance_rate']
            
            summary_data.append(row)
        
        # Create DataFrame and save
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_csv(summary_file, index=False)
        
        logger.info(f"Summary table generated: {summary_file}")
        return summary_file
        
    except Exception as e:
        logger.error(f"Failed to generate summary table: {str(e)}")
        return ""

def generate_json_report(audit_results: Dict[str, Any]) -> str:
    """
    Generate machine-readable JSON report
    
    Args:
        audit_results: Results from data quality audit
        
    Returns:
        Path to generated JSON file
    """
    logger = logging.getLogger('banking_audit')
    
    # Create date-based reports directory: reports/yyyy-mm-dd/
    base_reports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'reports')
    reports_dir = get_date_based_folder(base_reports_dir)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    json_file = os.path.join(reports_dir, f'audit_report_{timestamp}.json')
    
    try:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(audit_results, f, indent=2, default=str, ensure_ascii=False)
        
        logger.info(f"JSON report generated: {json_file}")
        return json_file
        
    except Exception as e:
        logger.error(f"Failed to generate JSON report: {str(e)}")
        return ""

# =====================================================
# DATA MAPPING
# =====================================================

def validate_data_structure(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Validate that data structure matches database schema exactly
    
    Args:
        data_dict: Dictionary from generate_data.py with table names as keys
        
    Returns:
        Validated data dictionary
    """
    logger = logging.getLogger('banking_audit')
    
    # Expected table names from schema.sql
    expected_tables = {
        'customer', 'face_template', 'bank_account', 
        'customer_device', 'transaction', 'authentication_log'
    }
    
    provided_tables = set(data_dict.keys())
    
    # Check if all expected tables are provided
    missing_tables = expected_tables - provided_tables
    extra_tables = provided_tables - expected_tables
    
    if missing_tables:
        logger.warning(f"Missing expected tables: {missing_tables}")
    
    if extra_tables:
        logger.warning(f"Unexpected extra tables: {extra_tables}")
    
    # Return only valid schema tables
    validated_data = {
        table_name: df for table_name, df in data_dict.items() 
        if table_name in expected_tables
    }
    
    logger.info(f"Data structure validation: {len(validated_data)}/{len(expected_tables)} expected tables found")
    return validated_data

# =====================================================
# MAIN AUDIT ORCHESTRATOR
# =====================================================

def run_audit_with_reports(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Run complete audit, cleaning, and report generation (NO DATABASE SAVE)
    For use in DAG quality_audit_task
    
    Args:
        data_dict: Dictionary of table_name -> DataFrame (from generate_data.py)
        
    Returns:
        Audit results dictionary with cleaning information and generated reports
    """
    logger = setup_logging()
    logger.info("Starting comprehensive banking data audit with intelligent cleaning...")
    
    try:
        # Step 1: Validate input data
        if not data_dict:
            raise Exception("No data provided for audit")
        
        total_records = sum(len(df) for df in data_dict.values())
        logger.info(f"Received data: {len(data_dict)} tables, {total_records} total records")
        
        # Step 2: Validate data structure matches database schema
        validated_data = validate_data_structure(data_dict)
        
        # Step 3: Run comprehensive data cleaning (audit + clean)
        cleaned_data, audit_results, cleaning_summary = run_comprehensive_data_cleaning(validated_data)
        
        # Step 4: Log cleaning summary
        logger.info("=" * 60)
        logger.info("DATA CLEANING SUMMARY")
        logger.info("=" * 60)
        total_original = 0
        total_final = 0
        
        for table_name, summary in cleaning_summary.items():
            total_original += summary['original_count']
            final_count = summary.get('final_count_after_fk_cleanup', summary['final_count'])
            total_final += final_count
            
            logger.info(f"{table_name}: {final_count}/{summary['original_count']} "
                       f"({summary.get('final_cleaned_percentage', summary['cleaned_percentage'])}%) clean")
        
        overall_clean_percentage = round((total_final / total_original) * 100, 2) if total_original > 0 else 0
        logger.info(f"OVERALL: {total_final}/{total_original} ({overall_clean_percentage}%) records retained")
        
        # Step 5: Generate reports (logs, CSV, JSON)
        logger.info("Generating audit reports...")
        
        log_file = generate_audit_log(audit_results)
        summary_file = generate_summary_table(audit_results)
        json_file = generate_json_report(audit_results)
        
        # Step 6: Update audit results with cleaning info (NO DATABASE SAVE)
        audit_results['data_cleaning'] = {
            'total_original_records': total_original,
            'total_clean_records': total_final,
            'overall_clean_percentage': overall_clean_percentage,
            'records_removed': total_original - total_final,
            'cleaning_approach': 'row_level_intelligent_cleaning'
        }
        
        # Add cleaned data to results for downstream tasks
        audit_results['cleaned_data'] = cleaned_data
        
        # Step 7: Log final summary (NO DATABASE SAVE STATUS)
        summary = audit_results['audit_summary']
        
        logger.info("=" * 80)
        logger.info("DATA AUDIT & CLEANING WITH REPORTS COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Audit Status: {summary['overall_status']}")
        logger.info(f"Checks Pass Rate: {summary['pass_rate']}%")
        logger.info(f"Checks: {summary['passed_checks']}/{summary['total_checks']} passed")
        logger.info(f"Data Quality: {total_final}/{total_original} ({overall_clean_percentage}%) clean records")
        logger.info("")
        logger.info("Generated Reports:")
        logger.info(f"  Detailed Log: {log_file}")
        logger.info(f"  Summary Table: {summary_file}")
        logger.info(f"  JSON Report: {json_file}")
        logger.info("")
        logger.info("Clean data prepared for downstream database loading task")
        logger.info("=" * 80)
        
        return audit_results
        
    except Exception as e:
        logger.error(f"Audit failed: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Return error result
        return {
            'audit_summary': {
                'audit_timestamp': datetime.now().isoformat(),
                'overall_status': 'ERROR',
                'error_message': str(e)
            },
            'check_results': {},
            'data_cleaning': {
                'error': str(e)
            }
        }

def save_data_only(cleaned_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Save clean data to database ONLY (for use in DAG load_data_task)
    
    Args:
        cleaned_data: Clean data dictionary from audit task
        
    Returns:
        Save operation results
    """
    logger = logging.getLogger('banking_audit')
    
    try:
        total_records = sum(len(df) for df in cleaned_data.values())
        
        if total_records == 0:
            logger.warning("No clean data to save - all records failed quality checks")
            return {
                'status': 'NO_CLEAN_DATA',
                'records_saved': 0,
                'message': 'All data failed quality checks'
            }
        
        logger.info(f"Saving {total_records} clean records to database...")
        save_success = save_clean_data_to_database(cleaned_data)
        
        if save_success:
            logger.info(f"Successfully saved {total_records} clean records to database")
            return {
                'status': 'SUCCESS',
                'records_saved': total_records,
                'message': f'Successfully saved {total_records} records'
            }
        else:
            logger.error("Failed to save clean data to database")
            return {
                'status': 'FAILED',
                'records_saved': 0,
                'message': 'Database save operation failed'
            }
            
    except Exception as e:
        error_msg = f"Database save failed: {str(e)}"
        logger.error(error_msg)
        return {
            'status': 'ERROR',
            'records_saved': 0,
            'message': error_msg
        }

def run_comprehensive_audit(data_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    LEGACY: Complete audit pipeline including database save (for standalone mode)
    
    Args:
        data_dict: Dictionary of table_name -> DataFrame (from generate_data.py)
        
    Returns:
        Audit results dictionary with cleaning information
    """
    # Run audit with reports
    audit_results = run_audit_with_reports(data_dict)
    
    if audit_results['audit_summary']['overall_status'] == 'ERROR':
        return audit_results
    
    # Extract clean data and save to database
    if 'cleaned_data' in audit_results:
        cleaned_data = audit_results['cleaned_data']
        save_results = save_data_only(cleaned_data)
        
        # Add database save status to audit results
        audit_results['database_save_status'] = save_results['status']
        audit_results['database_save_message'] = save_results['message']
        
        # Log final summary with database status
        logger = logging.getLogger('banking_audit')
        logger.info("=" * 80)
        logger.info("FULL AUDIT PIPELINE COMPLETED (STANDALONE MODE)")
        logger.info("=" * 80)
        logger.info(f"Database Save: {save_results['status']} - {save_results['message']}")
        logger.info("=" * 80)
    
    return audit_results

# =====================================================
# COMMAND LINE INTERFACE
# =====================================================

def main():
    """
    Main entry point for banking data audit (standalone mode)
    Note: This will generate data and then audit it
    """
    print("BANKING DATA QUALITY AUDIT SYSTEM")
    print("=" * 50)
    
    try:
        # Import and run data generation
        from generate_data import generate_data
        
        print("Generating banking data...")
        data_dict = generate_data()
        
        print("\nRunning data quality audit...")
        
        # Run audit
        results = run_comprehensive_audit(data_dict)
        
        # Exit with appropriate code
        if results['audit_summary']['overall_status'] == 'PASS':
            print("Audit completed successfully - All checks passed!")
            sys.exit(0)
        elif results['audit_summary']['overall_status'] == 'FAIL':
            print("Audit completed with issues - Some checks failed!")
            sys.exit(1)
        else:
            print("Audit failed due to error!")
            sys.exit(2)
            
    except Exception as e:
        print(f"Failed to run audit: {str(e)}")
        sys.exit(2)

if __name__ == "__main__":
    main()
