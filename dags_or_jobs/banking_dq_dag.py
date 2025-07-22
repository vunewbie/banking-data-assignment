import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException

# Add project src to Python path for imports
# src folder is mounted directly at /opt/airflow/src
sys.path.append('/opt/airflow/src')

# Import banking data modules
try:
    from generate_data import generate_data
    from monitoring_audit import run_audit_with_reports
    from monitoring_audit import save_data_only
    import logging
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure banking-data-assignment/src is in Python path")

# =====================================================
# DAG CONFIGURATION
# =====================================================

# DAG default arguments
default_args = {
    'owner': 'banking-data-assignment',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['vulocninh1@gmail.com']  # Update with actual email
}

# Create DAG
dag = DAG(
    dag_id='banking_data_quality_daily',
    default_args=default_args,
    description='Daily banking data quality monitoring and compliance checks',
    schedule_interval='0 2 * * *',  # Daily at 2:00 AM
    catchup=False,
    max_active_runs=1,
    tags=['banking', 'data-quality', 'compliance', 'daily']
)

# =====================================================
# UTILITY FUNCTIONS
# =====================================================
def setup_logging() -> logging.Logger:
    """Setup logging for DAG tasks"""
    logger = logging.getLogger('banking_dq_dag')
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger

def check_dependencies():
    """Check if all required dependencies are available"""
    logger = setup_logging()
    try:
        # Test that already imported modules are accessible
        if (callable(generate_data) and callable(run_audit_with_reports) and callable(save_data_only)):
            logger.info("All dependencies available")
            return True
        else:
            raise ImportError("Required functions not accessible")
    except (NameError, ImportError) as e:
        logger.error(f"Dependency check failed: {e}")
        raise AirflowException(f"Missing dependencies: {e}")

def send_alert_notification(context, message: str, alert_type: str = "ERROR"):
    """Send alert notifications for failures"""
    logger = setup_logging()
    logger.error(f"BANKING ALERT [{alert_type}]: {message}")
    
    print(f"\n{'='*60}")
    print(f"BANKING DATA QUALITY ALERT")
    print(f"Type: {alert_type}")
    print(f"Message: {message}")
    print(f"Time: {datetime.now()}")
    print(f"DAG: {context['dag'].dag_id}")
    print(f"Task: {context['task'].task_id}")
    print(f"{'='*60}\n")

# =====================================================
# DAG TASK FUNCTIONS
# =====================================================
def generate_banking_data(**context):
    """
    Task: Generate new banking data
    """
    logger = setup_logging()
    
    try:
        logger.info("Starting banking data generation...")
        
        # Generate data using existing function
        data = generate_data()
        
        # Get generation summary
        total_records = sum(len(df) for df in data.values())
        table_counts = {table: len(df) for table, df in data.items()}
        
        logger.info(f"Data generation completed successfully")
        logger.info(f"Generated {total_records} total records across {len(data)} tables")
        
        # Store results for downstream tasks
        generation_results = {
            "status": "success",
            "total_records": total_records,
            "table_counts": table_counts,
            "tables": list(data.keys()),
            "timestamp": datetime.now().isoformat()
        }
        
        # Pass data to next task via XCom
        context['task_instance'].xcom_push(key='generated_data', value=data)
        context['task_instance'].xcom_push(key='generation_results', value=generation_results)
        
        return generation_results
        
    except Exception as e:
        error_msg = f"Data generation failed: {str(e)}"
        logger.error(error_msg)
        send_alert_notification(context, error_msg, "CRITICAL")
        raise AirflowException(error_msg)

def run_data_quality_audit(**context):
    """
    Task: Run comprehensive data quality audit
    
    This is the core task that performs all quality checks
    """
    logger = setup_logging()
    
    try:
        logger.info("Starting banking data quality audit...")
        
        # Get data from previous task
        data = context['task_instance'].xcom_pull(task_ids='generate_data_task', key='generated_data')
        
        if data is None:
            # Fallback: generate fresh data (shouldn't happen normally)
            logger.warning("No data from previous task, generating fresh data as fallback...")
            data = generate_data()
        
        # Run comprehensive audit with reports generation (no database save)
        logger.info("Running comprehensive quality audit with report generation...")
        
        
        # Run full audit + cleaning + reports (no database save)
        audit_results = run_audit_with_reports(data)
        
        # Extract key metrics
        audit_summary = audit_results['audit_summary']
        
        logger.info(f"Audit completed - Status: {audit_summary['overall_status']}")
        logger.info(f"Pass rate: {audit_summary['pass_rate']}%")
        logger.info(f"Checks: {audit_summary['passed_checks']}/{audit_summary['total_checks']} passed")
        
        # Calculate clean data metrics
        if 'cleaned_data' in audit_results:
            cleaned_data = audit_results['cleaned_data']
            total_clean = sum(len(df) for df in cleaned_data.values())
            
            # Extract data cleaning info
            data_cleaning = audit_results.get('data_cleaning', {})
            total_original = data_cleaning.get('total_original_records', 0)
            clean_percentage = data_cleaning.get('overall_clean_percentage', 0)
            
            logger.info(f"Clean data: {total_clean}/{total_original} ({clean_percentage}%) records")
            logger.info("Reports generated: audit logs, summary tables, JSON reports")
            
            # Store results for downstream tasks
            context['task_instance'].xcom_push(key='audit_results', value=audit_results)
            context['task_instance'].xcom_push(key='cleaned_data', value=cleaned_data)
            
            return {
                'audit_results': audit_results,
                'cleaned_data': cleaned_data, 
                'total_clean_records': total_clean,
                'reports_generated': True
            }
        else:
            logger.error("No cleaned data found in audit results")
            raise AirflowException("Audit failed to produce cleaned data")
        
    except Exception as e:
        error_msg = f"Data quality audit failed: {str(e)}"
        logger.error(error_msg)
        send_alert_notification(context, error_msg, "CRITICAL")
        raise AirflowException(error_msg)

def load_clean_data_to_database(**context):
    """
    Task: Load clean data to database after quality checks pass
    """
    logger = setup_logging()
    
    try:
        # Get clean data from previous task
        cleaned_data = context['task_instance'].xcom_pull(task_ids='quality_audit_task', key='cleaned_data')
        cleaning_summary = context['task_instance'].xcom_pull(task_ids='quality_audit_task', key='cleaning_summary')
        
        if cleaned_data is None:
            raise AirflowException("No cleaned data found from quality audit task")
        
        # Calculate total clean records
        total_clean = sum(len(df) for df in cleaned_data.values())
        
        if total_clean == 0:
            logger.warning("No clean data to load - all records failed quality checks")
            return {
                'status': 'NO_CLEAN_DATA',
                'records_loaded': 0,
                'message': 'All data failed quality checks'
            }
        
        # Load clean data to database using dedicated function
        logger.info(f"Loading {total_clean} clean records to database...")
        
        
        save_results = save_data_only(cleaned_data)
        
        # Log results
        logger.info("=" * 50)
        logger.info("DATABASE LOAD SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Status: {save_results['status']}")
        logger.info(f"Records Loaded: {save_results['records_saved']}")
        logger.info(f"Message: {save_results['message']}")
        logger.info("=" * 50)
        
        return save_results
        
    except Exception as e:
        error_msg = f"Database loading failed: {str(e)}"
        logger.error(error_msg)
        send_alert_notification(context, error_msg, "CRITICAL")
        raise AirflowException(error_msg)

def evaluate_audit_results(**context):
    """
    Task: Evaluate audit results and determine next actions
    """
    logger = setup_logging()
    
    try:
        # Get audit results from previous task
        audit_results = context['task_instance'].xcom_pull(task_ids='quality_audit_task', key='audit_results')
        load_results = context['task_instance'].xcom_pull(task_ids='load_data_task', key='return_value')
        
        if audit_results is None:
            raise AirflowException("No audit results found from previous task")
        
        if load_results is None:
            raise AirflowException("No load results found from previous task")
        
        audit_summary = audit_results['audit_summary']
        overall_status = audit_summary['overall_status']
        pass_rate = audit_summary['pass_rate']
        failed_checks = audit_summary['failed_checks']
        
        # Database load status
        load_status = load_results.get('status', 'UNKNOWN')
        records_loaded = load_results.get('records_saved', 0)
        
        # Define pass rate thresholds
        warning_threshold = 80.0  # Below 80% triggers warning
        critical_threshold = 60.0  # Below 60% triggers critical alert
        
        evaluation = {
            "overall_status": overall_status,
            "pass_rate": pass_rate,
            "load_status": load_status,
            "records_loaded": records_loaded,
            "alert_level": "none",
            "action_required": False,
            "summary": f"Audit {overall_status} - {pass_rate}% pass rate, Database {load_status} - {records_loaded} records"
        }
        
        # Determine alert level and actions
        if load_status == "FAILED":
            evaluation["alert_level"] = "critical"
            evaluation["action_required"] = True
            evaluation["message"] = f"CRITICAL: Database loading failed - {load_results.get('message', 'Unknown error')}"
        elif load_status == "NO_CLEAN_DATA":
            evaluation["alert_level"] = "critical"
            evaluation["action_required"] = True
            evaluation["message"] = f"CRITICAL: No clean data to load - all records failed quality checks"
        elif overall_status == "FAIL":
            if pass_rate < critical_threshold:
                evaluation["alert_level"] = "critical"
                evaluation["action_required"] = True
                evaluation["message"] = f"CRITICAL: Data quality severely degraded ({pass_rate}% pass rate)"
            elif pass_rate < warning_threshold:
                evaluation["alert_level"] = "warning"
                evaluation["action_required"] = True
                evaluation["message"] = f"WARNING: Data quality below threshold ({pass_rate}% pass rate)"
            else:
                evaluation["alert_level"] = "info"
                evaluation["message"] = f"INFO: Some quality checks failed ({failed_checks} failures)"
        else:
            if load_status == "SUCCESS":
                evaluation["message"] = f"SUCCESS: All checks passed, {records_loaded} records loaded to database"
            else:
                evaluation["message"] = f"SUCCESS: Quality checks passed ({pass_rate}% pass rate), Database status: {load_status}"
        
        logger.info(f"Audit evaluation: {evaluation['message']}")
        
        # Store evaluation for alerting task
        context['task_instance'].xcom_push(key='evaluation', value=evaluation)
        
        return evaluation
        
    except Exception as e:
        error_msg = f"Audit evaluation failed: {str(e)}"
        logger.error(error_msg)
        send_alert_notification(context, error_msg, "ERROR")
        raise AirflowException(error_msg)

def send_alerts_if_needed(**context):
    """
    Task: Send alerts based on audit results
    """
    logger = setup_logging()
    
    try:
        # Get evaluation from previous task
        evaluation = context['task_instance'].xcom_pull(task_ids='evaluate_results_task', key='evaluation')
        
        if evaluation is None:
            logger.warning("No evaluation results found - skipping alerts")
            return {"status": "skipped", "reason": "no_evaluation"}
        
        alert_level = evaluation.get("alert_level", "none")
        message = evaluation.get("message", "Unknown status")
        
        if alert_level == "none":
            logger.info("No alerts needed - all checks passed")
            return {"status": "no_alerts_needed"}
        
        # Send appropriate level alert
        if alert_level in ["warning", "critical"]:
            send_alert_notification(context, message, alert_level.upper())
            
            # For critical alerts, also log additional details
            if alert_level == "critical":
                audit_results = context['task_instance'].xcom_pull(task_ids='quality_audit_task', key='audit_results')
                if audit_results:
                    failed_checks = []
                    for check_name, check_result in audit_results['check_results'].items():
                        if check_result['status'] == 'FAIL':
                            failed_checks.append(f"- {check_name}: {len(check_result.get('issues', []))} issues")
                    
                    if failed_checks:
                        detail_msg = f"Failed checks:\n" + "\n".join(failed_checks)
                        logger.error(detail_msg)
        
        return {
            "status": "alerts_sent",
            "alert_level": alert_level,
            "message": message
        }
        
    except Exception as e:
        error_msg = f"Alert sending failed: {str(e)}"
        logger.error(error_msg)
        # Don't fail the DAG for alert issues
        return {"status": "alert_error", "error": str(e)}

# =====================================================
# DAG TASK DEFINITIONS
# =====================================================

# Task 0: Check dependencies
check_deps_task = PythonOperator(
    task_id='check_dependencies_task',
    python_callable=check_dependencies,
    dag=dag,
    doc_md="""
    ## Check Dependencies
    
    Validates that all required Python modules and dependencies are available.
    This task will fail the DAG early if critical dependencies are missing.
    """
)

# Task 1: Generate data (optional)
generate_data_task = PythonOperator(
    task_id='generate_data_task',
    python_callable=generate_banking_data,
    dag=dag,
    doc_md="""
    ## Generate Banking Data
    
    Optionally generates new banking data for quality testing.
    Can be enabled/disabled via Airflow Variable: `banking_dq_generate_data`
    
    **Default**: Disabled (uses existing data or generates fresh for audit)
    """
)

# Task 2: Run quality audit (core task)
quality_audit_task = PythonOperator(
    task_id='quality_audit_task',
    python_callable=run_data_quality_audit,
    dag=dag,
    doc_md="""
    ## Data Quality Audit
    
    Runs comprehensive data quality checks including:
    - Null/missing values validation
    - Uniqueness constraints 
    - Format/length validation
    - Foreign key integrity
    - Risk-based compliance checks (2345/QĐ-NHNN 2023)
    
    **This is the core task of the DAG.**
    """
)

# Task 3: Load clean data to database
load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_clean_data_to_database,
    retries=3,  # More retries for database operations
    retry_delay=timedelta(minutes=3),
    dag=dag,
    doc_md="""
    ## Load Clean Data to Database
    
    Loads verified clean data to the banking database after quality checks pass.
    
    **Features:**
    - Only loads data that passed ALL quality checks
    - Handles database connection errors with retries
    - Tracks loading statistics and failures
    - Provides detailed loading summary
    
    **Dependencies:** Must run after quality_audit_task
    """
)

# Task 4: Evaluate results
evaluate_results_task = PythonOperator(
    task_id='evaluate_results_task',
    python_callable=evaluate_audit_results,
    dag=dag,
    doc_md="""
    ## Evaluate Audit Results
    
    Analyzes audit results and determines:
    - Overall health status
    - Alert levels (none/info/warning/critical)
    - Required actions
    
    **Thresholds**:
    - Warning: < 80% pass rate
    - Critical: < 60% pass rate
    """
)

# Task 4: Send alerts
alert_task = PythonOperator(
    task_id='alert_task',
    python_callable=send_alerts_if_needed,
    dag=dag,
    doc_md="""
    ## Send Alerts
    
    Sends notifications based on audit results:
    - Logs all outcomes
    - Sends alerts for warnings/critical issues
    - Integrates with monitoring systems
    
    **Alert channels**: Logs, Email, Slack (configurable)
    """
)

# Task 5: Cleanup (optional)
cleanup_task = BashOperator(
    task_id='cleanup_task',
    bash_command="""
    echo "Banking DQ DAG completed at $(date)"
    echo "Cleaning up temporary files..."
    # Add any cleanup commands here
    echo "Cleanup completed"
    """,
    dag=dag,
    doc_md="""
    ## Cleanup
    
    Performs any necessary cleanup after DAG completion:
    - Logs completion time
    - Cleans temporary files
    - Updates monitoring status
    """
)

# =====================================================
# DAG TASK DEPENDENCIES
# =====================================================

# Define task flow
check_deps_task >> generate_data_task >> quality_audit_task >> load_data_task >> evaluate_results_task >> alert_task >> cleanup_task
