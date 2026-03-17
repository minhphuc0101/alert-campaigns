import os
import smtplib
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime
from datetime import timedelta
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SENDER_EMAIL = os.getenv("EMAIL_SENDER_ADDRESS")
SENDER_PASSWORD = os.getenv("EMAIL_SENDER_PASSWORD")
RECIPIENT_EMAIL = "phuc.tran@digimind.asia"

# Google Sheets Info
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
TAB_NAME = os.getenv("GOOGLE_SHEET_TAB_NAME")

# Settings
LOOKBACK_DAYS = 7


def get_sheets_client():
    """Authenticates and returns a gspread client."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]
    
    try:
        credentials = Credentials.from_service_account_file(
            "service_account.json", scopes=scopes
        )
        client = gspread.authorize(credentials)
        return client
    except FileNotFoundError:
        print("Error: 'service_account.json' not found.")
        exit(1)


def fetch_recent_data(client):
    """Fetches data from the Google Sheet and filters for the last LOOKBACK_DAYS."""
    try:
        sheet = client.open_by_key(SHEET_ID).worksheet(TAB_NAME)
    except Exception as e:
        print(f"Error opening Google Sheet: {e}")
        exit(1)

    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    if df.empty:
        print("The sheet is empty.")
        return None

    # Standardize column names
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Find the date/day column
    date_keywords = ['date', 'day', 'time', 'period']
    date_col = next((col for col in df.columns if any(kw in col for kw in date_keywords)), None)
            
    if not date_col:
        print(f"Error: Could not find a date column. Headers: {df.columns.tolist()}")
        return None

    # Convert to datetime
    df[date_col] = pd.to_datetime(df[date_col], format='mixed', dayfirst=False)

    # Filter for the last 7 days
    cutoff_date = pd.Timestamp.now().normalize() - timedelta(days=LOOKBACK_DAYS)
    recent_df = df[df[date_col] >= cutoff_date]
    recent_df = recent_df.sort_values(by=date_col, ascending=False)
    
    return recent_df, date_col


def extract_kpi_target(campaign_name):
    """Extracts numerical target from name (e.g. '3000 CPE' -> 3000, 'CPM_100' -> 100)."""
    # Look for patterns like '3000 CPE', 'CPM_100', '700 CPM', etc.
    cpe_match = re.search(r'(\d+)\s*CPE|CPE[_\s]*(\d+)', campaign_name, re.I)
    cpm_match = re.search(r'(\d+)\s*CPM|CPM[_\s]*(\d+)', campaign_name, re.I)
    
    if cpe_match:
        return 'CPE', int(cpe_match.group(1) or cpe_match.group(2))
    if cpm_match:
        # CPM is volume: X * 1000 impressions
        return 'CPM', int(cpm_match.group(1) or cpm_match.group(2)) * 1000
        
    return None, None


def analyze_data(df, date_col):
    """Performs mathematical analysis for alerts."""
    alerts = []
    
    # Exclude App Installs and Leads
    df = df[~df['campaign name'].str.contains('app install|lead', case=False, na=False)]
    
    campaigns = df['campaign name'].unique()
    
    # Group dates to find 'Yesterday' (latest date) and 'Prior 3 Days'
    available_dates = sorted(df[date_col].unique(), reverse=True)
    if not available_dates:
        return []
        
    latest_date = available_dates[0]
    prior_3_dates = available_dates[1:4]
    
    for campaign in campaigns:
        campaign_df = df[df['campaign name'] == campaign]
        status = campaign_df.iloc[0].get('campaign status', 'Unknown')
        
        # 1. KPI Check (7-Day Aggregate)
        kpi_type, kpi_target = extract_kpi_target(campaign)
        if kpi_type and kpi_target:
            total_spent = campaign_df['amount spent'].sum()
            total_engagement = campaign_df['post engagement'].sum()
            total_impressions = campaign_df['impressions'].sum()
            
            if kpi_type == 'CPE':
                actual_cpe = total_spent / total_engagement if total_engagement > 0 else 0
                # Alert only if Actual > Target + 5%
                if actual_cpe > (kpi_target * 1.05):
                    alerts.append({
                        'campaign': campaign,
                        'status': status,
                        'issue': f"Target {kpi_target} CPE, actual {actual_cpe:,.0f} CPE",
                        'metrics': f"Spent: {total_spent:,.0f}, Engagement: {total_engagement:,.0f}",
                        'reason': "Target KPI exceeded (>5%)"
                    })
            elif kpi_type == 'CPM':
                # Volume target: Impressions reached
                # Alert only if Actual < Target - 5%
                if total_impressions < (kpi_target * 0.95):
                    alerts.append({
                        'campaign': campaign,
                        'status': status,
                        'issue': f"Target {kpi_target:,.0f} Impressions, actual {total_impressions:,.0f}",
                        'metrics': f"Spent: {total_spent:,.0f}",
                        'reason': "Impression target not met (>5% gap)"
                    })

        # 2. Spend Anomaly (Yesterday vs Avg 3 Days)
        yesterday_data = campaign_df[campaign_df[date_col] == latest_date]
        prior_data = campaign_df[campaign_df[date_col].isin(prior_3_dates)]
        
        if not yesterday_data.empty and not prior_data.empty:
            yesterday_spend = yesterday_data['amount spent'].sum()
            avg_prior_spend = prior_data['amount spent'].sum() / len(prior_3_dates)
            
            if avg_prior_spend > 0:
                percent_increase = (yesterday_spend - avg_prior_spend) / avg_prior_spend
                if percent_increase > 0.30: # 30% jump
                    alerts.append({
                        'campaign': campaign,
                        'status': status,
                        'issue': f"Yesterday spend ({yesterday_spend:,.0f}) is {percent_increase*100:.1f}% higher than 3-day avg ({avg_prior_spend:,.0f})",
                        'metrics': f"Date: {latest_date.strftime('%Y-%m-%d')}",
                        'reason': "High spend anomaly"
                    })

    return alerts


def format_email(alerts):
    if not alerts:
        return None, "Spending is within normal parameters and no action is required today."
        
    subject = f"🚨 Campaign Alert: {len(alerts)} Campaigns Require Attention"
    
    body = "Hi Team,\n\nThe following campaigns are currently exceeding their target KPIs or spending anomalies have been detected:\n\n"
    
    for i, alert in enumerate(alerts, 1):
        body += f"{i}. {alert['campaign']} ({alert['status']})\n"
        body += f"   - 📉 Issue: {alert['issue']}\n"
        body += f"   - 💰 Spent: {alert['metrics']}\n"
        body += f"   - 🏷️ Reason: {alert['reason']}\n\n"
        
    body += "---\nPlease review your Ads Manager.\n- Alert System"
    
    return subject, body


def send_email(subject, body):
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        return print("Error: Email credentials missing.")

    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        server.quit()
        print(f"Email sent to {RECIPIENT_EMAIL}")
    except Exception as e:
        print(f"Error sending email: {e}")


def main():
    print(f"Starting Campaign Alert Script (v2.0 - Pure Python) at {datetime.datetime.now()}")
    client = get_sheets_client()
    data_tuple = fetch_recent_data(client)
    
    if not data_tuple:
        return
    
    df, date_col = data_tuple
    alerts = analyze_data(df, date_col)
    
    subject, body = format_email(alerts)
    
    if subject:
        print(f"Alerts found! Sending email...")
        send_email(subject, body)
    else:
        print(body)

if __name__ == "__main__":
    main()
