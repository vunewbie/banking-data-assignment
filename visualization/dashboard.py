import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set page config
st.set_page_config(
    page_title="Banking Data Quality Dashboard",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for banking theme
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f4e79;
        text-align: center;
        margin-bottom: 2rem;
        border-bottom: 3px solid #1f4e79;
        padding-bottom: 1rem;
    }
    .section-header {
        font-size: 1.5rem;
        color: #2c5282;
        margin: 1rem 0;
        border-left: 4px solid #2c5282;
        padding-left: 1rem;
    }
    .metric-card {
        background-color: #f7fafc;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

class BankingDashboard:
    def __init__(self):
        self.db_connection = None
        self.setup_database_connection()
    
    def setup_database_connection(self):
        """Thiết lập kết nối database"""
        try:
            self.db_connection = psycopg2.connect(
                host="localhost",
                port=5433,
                database="banking_system",
                user="postgres",
                password="postgres"
            )
            st.sidebar.success("✅ Database Connected")
        except Exception as e:
            st.sidebar.error(f"❌ Database Connection Failed: {str(e)}")
            self.db_connection = None
    
    def load_audit_reports(self):
        """Load latest audit report JSON file based on timestamp inside latest reports/yyyy-mm-dd folder"""
        project_root = Path(__file__).resolve().parent.parent
        reports_dir = project_root / "reports"

        if not reports_dir.exists():
            st.sidebar.warning("❌ Reports directory not found.")
            return None

        # Step 1: Find latest date folder (yyyy-mm-dd)
        latest_folder = None
        for folder in reports_dir.iterdir():
            if folder.is_dir():
                try:
                    datetime.strptime(folder.name, "%Y-%m-%d")
                    if latest_folder is None or folder.name > latest_folder.name:
                        latest_folder = folder
                except ValueError:
                    continue

        if not latest_folder:
            return None

        # Step 2: Find latest audit_report_*.json inside that folder
        latest_json = None
        latest_time = None
        for file in latest_folder.glob("audit_report_*.json"):
            try:
                timestamp_str = file.stem.split("audit_report_")[1]
                timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                if latest_time is None or timestamp > latest_time:
                    latest_json = file
                    latest_time = timestamp
            except Exception:
                continue
        
        if latest_json and latest_json.exists():
            with open(latest_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data

        return None

    def get_failed_checks_data(self):
        """Analyze failed checks from audit reports"""
        audit_data = self.load_audit_reports()
        if not audit_data:
            return pd.DataFrame()

        failed_checks = []

        check_results = audit_data.get("check_results", {})
        for check_name, result in check_results.items():
            if isinstance(result, dict) and result.get("status") == "FAIL":
                total_records = 0
                failed_count = 0

                summary = result.get("summary", {})
                for table_summary in summary.values():
                    if isinstance(table_summary, dict):  # Chỉ cộng nếu là dict
                        total_records += table_summary.get("total_records", 0)
                        failed_count += table_summary.get("failed_records", 0)

                failed_checks.append({
                    "Check Name": check_name,
                    "Failed Count": failed_count,
                    "Total Records": total_records,
                    "Failure Rate": (failed_count / max(total_records, 1)) * 100,
                    "Severity": (
                        "High" if failed_count > 100 else
                        "Medium" if failed_count > 10 else
                        "Low"
                    )
                })

        return pd.DataFrame(failed_checks)

    def get_risky_transactions_data(self):
        """Get risky transactions data from database"""
        if not self.db_connection:
            return pd.DataFrame()
        
        query = """
        SELECT 
            t.transaction_id,
            t.amount,
            t.transaction_type,
            t.authentication_method,
            c.risk_rating,
            c.risk_score,
            t.created_at,
            CASE 
                WHEN t.amount >= 10000000 THEN 'High Risk'
                WHEN t.amount >= 5000000 THEN 'Medium Risk'
                ELSE 'Low Risk'
            END as risk_category
        FROM transaction t
        JOIN bank_account ba ON t.account_id = ba.account_id
        JOIN customer c ON ba.customer_id = c.customer_id
        WHERE is_fraud = True
        ORDER BY t.amount DESC, c.risk_score DESC
        LIMIT 1000;
        """
        
        try:
            df = pd.read_sql(query, self.db_connection)
            df['created_at'] = pd.to_datetime(df['created_at'])
            return df
        except Exception as e:
            st.error(f"Error loading risky transactions: {str(e)}")
            return pd.DataFrame()
    
    def get_unverified_devices_data(self):
        """Get unverified devices data from database"""
        if not self.db_connection:
            return pd.DataFrame()
        
        query = """
        SELECT 
            cd.device_identifier,
            cd.device_type,
            cd.is_trusted,
            cd.status,
            c.customer_id,
            c.full_name,
            c.risk_rating,
            c.risk_score,
            cd.first_seen_at,
            cd.last_used_at,
            CASE 
                WHEN cd.status = 'Blocked' THEN 'High Risk'
                WHEN cd.is_trusted = false THEN 'Medium Risk'
                ELSE 'Low Risk'
            END as device_risk_level
        FROM customer_device cd
        JOIN customer c ON cd.customer_id = c.customer_id
        WHERE cd.is_trusted = false OR cd.status != 'Active'
        ORDER BY 
            CASE cd.status WHEN 'Blocked' THEN 1 WHEN 'Expired' THEN 2 ELSE 3 END,
            c.risk_score DESC;
        """
        
        try:
            df = pd.read_sql(query, self.db_connection)
            df['first_seen_at'] = pd.to_datetime(df['first_seen_at'])
            df['last_used_at'] = pd.to_datetime(df['last_used_at'])
            return df
        except Exception as e:
            st.error(f"Error loading untrusted devices: {str(e)}")
            return pd.DataFrame()
    
    def render_sidebar(self):
        """Render sidebar controls"""
        st.sidebar.markdown("## 🎛️ Dashboard Controls")
        
        # Date range picker
        st.sidebar.markdown("### 📅 Date Range")
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=[datetime.now() - timedelta(days=30), datetime.now()],
            key="date_range"
        )
        
        # Risk level filter
        st.sidebar.markdown("### 🎯 Risk Level Filter")
        risk_levels = st.sidebar.multiselect(
            "Select Risk Levels",
            options=['High Risk', 'Medium Risk', 'Low Risk'],
            default=['High Risk', 'Medium Risk'],
            key="risk_filter"
        )
        
        # Amount range for transactions
        st.sidebar.markdown("### 💰 Transaction Amount Range")
        amount_range = st.sidebar.slider(
            "Amount Range (VND)",
            min_value=0,
            max_value=50000000,
            value=(1000000, 20000000),
            step=1000000,
            format="%d",
            key="amount_range"
        )
        
        # Auto refresh
        st.sidebar.markdown("### 🔄 Auto Refresh")
        auto_refresh = st.sidebar.checkbox("Enable Auto Refresh (30s)", key="auto_refresh")
        
        if auto_refresh:
            st.rerun()
        
        # Refresh button
        if st.sidebar.button("🔄 Refresh Data", key="refresh_btn"):
            st.rerun()
        
        return {
            'date_range': date_range,
            'risk_levels': risk_levels,
            'amount_range': amount_range,
            'auto_refresh': auto_refresh
        }
    
    def render_main_dashboard(self, filters):
        """Render main dashboard content"""
        
        # Main header
        st.markdown('<h1 class="main-header">🏦 Banking Data Quality Dashboard</h1>', unsafe_allow_html=True)
        
        # Last update time
        col1, col2, col3 = st.columns([2, 1, 1])
        with col3:
            st.metric("🕒 Last Updated", datetime.now().strftime("%H:%M:%S"))
        
        # Section 1: Top Failed Checks
        self.render_failed_checks_section()
        
        st.divider()
        
        # Section 2: Risky Transactions
        self.render_risky_transactions_section(filters)
        
        st.divider()
        
        # Section 3: Untrusted Devices
        self.render_unverified_devices_section(filters)
    
    def render_failed_checks_section(self):
        """Render failed checks analysis section"""
        st.markdown('<div class="section-header">🔴 Top Failed Data Quality Checks</div>', unsafe_allow_html=True)
        
        failed_checks_df = self.get_failed_checks_data()
        
        if failed_checks_df.empty:
            st.warning("📊 No failed checks data available. Run the data quality audit first.")
            return
        
        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_failed = failed_checks_df['Failed Count'].sum()
            st.metric("🚨 Total Failed Records", f"{total_failed:,}")
        
        with col2:
            avg_failure_rate = failed_checks_df['Failure Rate'].mean()
            st.metric("📈 Avg Failure Rate", f"{avg_failure_rate:.1f}%")
        
        with col3:
            critical_checks = len(failed_checks_df[failed_checks_df['Severity'] == 'High'])
            st.metric("⚠️ Critical Checks", critical_checks)
        
        with col4:
            total_checks = len(failed_checks_df)
            st.metric("🔍 Total Checks", total_checks)
        
        # Charts row
        col1, col2 = st.columns(2)
        
        with col1:
            # Bar chart of failed checks
            if not failed_checks_df.empty:
                fig_bar = px.bar(
                    failed_checks_df.head(10),
                    x='Failed Count',
                    y='Check Name',
                    color='Severity',
                    title="🔴 Top 10 Failed Checks by Count",
                    orientation='h',
                    color_discrete_map={
                        'High': '#ef4444',
                        'Medium': '#f97316', 
                        'Low': '#eab308'
                    }
                )
                fig_bar.update_layout(height=400)
                st.plotly_chart(fig_bar, use_container_width=True)
        
        with col2:
            # Pie chart of failure distribution
            if not failed_checks_df.empty:
                severity_counts = failed_checks_df['Severity'].value_counts()
                fig_pie = px.pie(
                    values=severity_counts.values,
                    names=severity_counts.index,
                    title="📊 Failure Distribution by Severity",
                    color_discrete_map={
                        'High': '#ef4444',
                        'Medium': '#f97316',
                        'Low': '#eab308'
                    }
                )
                fig_pie.update_layout(height=400)
                st.plotly_chart(fig_pie, use_container_width=True)
        
        # Detailed table
        with st.expander("📋 Detailed Failed Checks Data"):
            st.dataframe(failed_checks_df, use_container_width=True)
    
    def render_risky_transactions_section(self, filters):
        """Render risky transactions analysis section"""
        st.markdown('<div class="section-header">⚠️ High-Risk Transaction Analysis</div>', unsafe_allow_html=True)
        
        risky_df = self.get_risky_transactions_data()
        
        if risky_df.empty:
            st.warning("📊 No risky transactions data available.")
            return
        
        # Apply filters
        if filters['risk_levels']:
            risky_df = risky_df[risky_df['risk_category'].isin(filters['risk_levels'])]
        
        if len(filters['amount_range']) == 2:
            min_amount, max_amount = filters['amount_range']
            risky_df = risky_df[(risky_df['amount'] >= min_amount) & (risky_df['amount'] <= max_amount)]
        
        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_amount = risky_df['amount'].sum()
            st.metric("💰 Total Risky Amount", f"{total_amount/1000000:.1f}M VND")
        
        with col2:
            transaction_count = len(risky_df)
            st.metric("📊 Risky Transactions", f"{transaction_count:,}")
        
        with col3:
            avg_risk_score = risky_df['risk_score'].mean()
            st.metric("🎯 Avg Risk Score", f"{avg_risk_score:.1f}")
        
        with col4:
            high_risk_count = len(risky_df[risky_df['risk_category'] == 'High Risk'])
            st.metric("🚨 High Risk Count", high_risk_count)
        
        # Charts row
        col1, col2 = st.columns(2)
        
        with col1:
            # Time series of risky transactions
            if not risky_df.empty:
                daily_amounts = risky_df.groupby(risky_df['created_at'].dt.date)['amount'].sum().reset_index()
                fig_time = px.line(
                    daily_amounts,
                    x='created_at',
                    y='amount',
                    title="📈 Daily Risky Transaction Volume",
                    labels={'amount': 'Amount (VND)', 'created_at': 'Date'}
                )
                fig_time.update_layout(height=400)
                st.plotly_chart(fig_time, use_container_width=True)
        
        with col2:
            # Scatter plot: Amount vs Risk Score
            if not risky_df.empty:
                fig_scatter = px.scatter(
                    risky_df,
                    x='risk_score',
                    y='amount',
                    color='risk_category',
                    size='amount',
                    title="🎯 Amount vs Risk Score",
                    labels={'amount': 'Amount (VND)', 'risk_score': 'Risk Score'},
                    color_discrete_map={
                        'High Risk': '#ef4444',
                        'Medium Risk': '#f97316',
                        'Low Risk': '#eab308'
                    }
                )
                fig_scatter.update_layout(height=400)
                st.plotly_chart(fig_scatter, use_container_width=True)
        
        # Top transactions table
        with st.expander("💰 Top 20 Highest Risk Transactions"):
            top_transactions = risky_df.nlargest(20, 'amount')[['transaction_id', 'amount', 'transaction_type', 'authentication_method', 'risk_rating', 'risk_score', 'created_at']]
            st.dataframe(top_transactions, use_container_width=True)
    
    def render_unverified_devices_section(self, filters):
        """Render untrusted devices analysis section"""
        st.markdown('<div class="section-header">📱 Untrusted Devices by Customer</div>', unsafe_allow_html=True)
        
        devices_df = self.get_unverified_devices_data()
        
        if devices_df.empty:
            st.warning("📊 No untrusted devices data available.")
            return
        
        # Apply risk level filter
        if filters['risk_levels']:
            devices_df = devices_df[devices_df['device_risk_level'].isin(filters['risk_levels'])]
        
        # Key metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_devices = len(devices_df)
            st.metric("📱 Untrusted Devices", f"{total_devices:,}")
        
        with col2:
            unique_customers = devices_df['customer_id'].nunique()
            st.metric("👥 Affected Customers", unique_customers)
        
        with col3:
            trusted_percentage = (devices_df['is_trusted'].sum() / len(devices_df) * 100) if len(devices_df) > 0 else 0
            st.metric("🎯 Trusted %", f"{trusted_percentage:.1f}%")
        
        with col4:
            high_risk_devices = len(devices_df[devices_df['device_risk_level'] == 'High Risk'])
            st.metric("🚨 Blocked Devices", high_risk_devices)
        
        # Charts row
        col1, col2 = st.columns(2)
        
        with col1:
            # Device types distribution
            if not devices_df.empty:
                device_types = devices_df['device_type'].value_counts()
                fig_device_types = px.pie(
                    values=device_types.values,
                    names=device_types.index,
                    title="📱 Untrusted Devices by Type"
                )
                fig_device_types.update_layout(height=400)
                st.plotly_chart(fig_device_types, use_container_width=True)
        
        with col2:
            # Customers with most unverified devices
            if not devices_df.empty:
                customer_device_counts = devices_df.groupby(['customer_id', 'full_name']).size().reset_index(name='device_count')
                top_customers = customer_device_counts.nlargest(10, 'device_count')
                
                fig_customers = px.bar(
                    top_customers,
                    x='device_count',
                    y='full_name',
                    title="👥 Top 10 Customers with Most Untrusted Devices",
                    orientation='h'
                )
                fig_customers.update_layout(height=400)
                st.plotly_chart(fig_customers, use_container_width=True)
        
        # Device status and trust analysis
        col1, col2 = st.columns(2)
        
        with col1:
            # Device status distribution
            if not devices_df.empty:
                status_counts = devices_df['status'].value_counts()
                fig_status = px.pie(
                    values=status_counts.values,
                    names=status_counts.index,
                    title="📊 Device Status Distribution",
                    color_discrete_map={
                        'Active': '#22c55e',
                        'Blocked': '#ef4444',
                        'Expired': '#f97316'
                    }
                )
                fig_status.update_layout(height=400)
                st.plotly_chart(fig_status, use_container_width=True)
        
        with col2:
            # Risk level distribution
            if not devices_df.empty:
                risk_counts = devices_df['device_risk_level'].value_counts()
                fig_risk = px.bar(
                    x=risk_counts.index,
                    y=risk_counts.values,
                    title="🎯 Device Risk Level Distribution",
                    color=risk_counts.index,
                    color_discrete_map={
                        'High Risk': '#ef4444',
                        'Medium Risk': '#f97316',
                        'Low Risk': '#eab308'
                    }
                )
                fig_risk.update_layout(height=400)
                st.plotly_chart(fig_risk, use_container_width=True)
        
        # Detailed devices table
        with st.expander("📱 Detailed Untrusted Devices Data"):
            display_columns = ['device_identifier', 'device_type', 'is_trusted', 'status', 'full_name', 'device_risk_level', 'first_seen_at', 'last_used_at']
            st.dataframe(devices_df[display_columns], use_container_width=True)

def main():
    """Main function to run the dashboard"""
    
    # Initialize dashboard
    dashboard = BankingDashboard()
    
    # Render sidebar
    filters = dashboard.render_sidebar()
    
    # Render main dashboard
    dashboard.render_main_dashboard(filters)
    
    # Footer
    st.markdown("---")
    st.markdown("🏦 **Banking Data Quality Dashboard** | Powered by Streamlit | Real-time Banking Analytics")

if __name__ == "__main__":
    main()
