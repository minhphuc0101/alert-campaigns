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


def fetch_spreadsheet_data(client):
    """Fetches data from the Google Sheet and prepares it for analysis."""
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

    # Find the date column
    date_keywords = ['date', 'day']
    date_col = next((col for col in df.columns if any(kw in col for kw in date_keywords)), None)
            
    if not date_col:
        print(f"Error: Could not find a date column. Headers: {df.columns.tolist()}")
        return None

    # Convert to datetime
    df[date_col] = pd.to_datetime(df[date_col], format='mixed', dayfirst=False)
    
    # Sort by date
    df = df.sort_values(by=date_col, ascending=False)
    
    # Find the status column
    status_keywords = ['status', 'state']
    status_col = next((col for col in df.columns if any(kw in col for kw in status_keywords)), None)
    
    return df, date_col, status_col


def extract_kpi_target(campaign_name):
    """Extracts numerical target from name (e.g. '3000 CPE' -> 3000, 'CPM_100' -> 100)."""
    # Look for patterns like '3000 CPE', 'CPM_100', '700 CPM'...
    cpe_match = re.search(r'(\d+[,\d]*)\s*CPE|CPE[_\s]*(\d+[,\d]*)', campaign_name, re.I)
    cpm_match = re.search(r'(\d+[,\d]*)\s*CPM|CPM[_\s]*(\d+[,\d]*)', campaign_name, re.I)
    
    def parse_num(val):
        if not val: return None
        return int(val.replace(',', ''))

    if cpe_match:
        val = parse_num(cpe_match.group(1) or cpe_match.group(2))
        return 'CPE', val
    if cpm_match:
        val = parse_num(cpm_match.group(1) or cpm_match.group(2))
        # CPM is volume: X * 1000 impressions
        return 'CPM', val * 1000
        
    return None, None


def analyze_data(df, date_col, status_col):
    """Performs mathematical analysis for alerts."""
    alerts = []
    
    # Exclude App Installs and Leads (use regex for better matching)
    df = df[~df['campaign name'].str.contains('app install|lead', case=False, na=False)]
    
    campaigns = df['campaign name'].unique()
    available_dates = df[date_col].drop_duplicates().sort_values(ascending=False).tolist()
    if not available_dates:
        return []
        
    latest_date = available_dates[0]
    prior_3_dates = available_dates[1:4]
    
    for campaign in campaigns:
        campaign_df = df[df['campaign name'] == campaign]
        latest_row = campaign_df.iloc[0]
        
        # Determine status
        if status_col:
             status = str(latest_row.get(status_col, 'unknown')).strip()
        else:
             status = latest_row.get('campaign status', 'unknown')
        
        status = status.lower() if isinstance(status, str) else 'unknown'
        
        total_spent = campaign_df['amount spent'].sum()
        total_engagement = campaign_df['post engagement'].sum()
        total_impressions = campaign_df['impressions'].sum()
        total_reach = campaign_df['reach'].sum()
        total_video_views = campaign_df.get('3-second video views', pd.Series([0])).sum()
        
        # 1. Lifetime Volume KPI Check
        kpi_type, kpi_target = extract_kpi_target(campaign)
        if kpi_type and kpi_target:
            if kpi_type == 'CPE':
                if total_engagement > kpi_target:
                    alerts.append({
                        'campaign': campaign,
                        'status': status,
                        'issue': f"Volume Target reached (Target: {kpi_target:,} engagement, Actual: {total_engagement:,.0f})",
                        'spent_line': f"{total_spent:,.0f} (Lifetime)",
                        'metrics': f"Total Post Engagement: {total_engagement:,.0f}, Reach: {total_reach:,.0f}, Impressions: {total_impressions:,.0f}",
                        'reason': "Engagement target achieved"
                    })
            elif kpi_type == 'CPM':
                if total_impressions > kpi_target:
                    alerts.append({
                        'campaign': campaign,
                        'status': status,
                        'issue': f"Volume Target reached (Target: {kpi_target:,.0f} impressions, Actual: {total_impressions:,.0f})",
                        'spent_line': f"{total_spent:,.0f} (Lifetime)",
                        'metrics': f"Total Impressions: {total_impressions:,.0f}, Reach: {total_reach:,.0f}",
                        'reason': "Impression target achieved"
                    })

        # 2. Spend Cap Check
        spend_cap = latest_row.get('campaign spend cap', 0)
        try:
            if isinstance(spend_cap, str):
                spend_cap = float(spend_cap.replace(',', ''))
            else:
                spend_cap = float(spend_cap)
        except (ValueError, TypeError):
            spend_cap = 0

        if spend_cap > 0 and total_spent > spend_cap:
             alerts.append({
                'campaign': campaign,
                'status': status,
                'issue': f"Spending exceeds cap. Spent: {total_spent:,.0f} (Cap: {spend_cap:,.0f})",
                'spent_line': f"{total_spent:,.0f} (Cap: {spend_cap:,.0f})",
                'metrics': f"Reach: {total_reach:,.0f}, Impressions: {total_impressions:,.0f}, Post Engagement: {total_engagement:,.0f}",
                'reason': "Spending exceeds cap"
            })

        # 3. Spend Anomaly (Yesterday vs Avg 3 Days)
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
                        'spent_line': f"Yesterday: {yesterday_spend:,.0f} (Avg: {avg_prior_spend:,.0f})",
                        'metrics': f"Date: {latest_date.strftime('%Y-%m-%d')}",
                        'reason': "High spend anomaly"
                    })

    # Remove duplicates if a campaign triggered multiple alerts
    unique_alerts = []
    seen = set()
    for a in alerts:
        key = f"{a['campaign']}_{a['reason']}"
        if key not in seen:
            unique_alerts.append(a)
            seen.add(key)
    return unique_alerts


def format_email(alerts):
    if not alerts:
        return None, "Spending is within normal parameters and no action is required today."
        
    subject = f"🚨 Action Required: Campaign Alert [V5.0 - VOLUME ONLY] - {datetime.datetime.now().strftime('%Y-%m-%d')}"
    body = "Hi Team,\n\nThe following campaigns are currently exceeding their target KPIs or spending anomalies have been detected:\n\n"
    
    for i, alert in enumerate(alerts, 1):
        body += f"{i}. {alert['campaign']}\n"
        body += f"   - ❗ Issue: {alert['issue']}\n"
        body += f"   - 💰 Spent: {alert['spent_line']}\n"
        body += f"   - 📊 Metrics: {alert['metrics']}\n"
        body += f"   - 🚦 Campaign Status: {alert['status']}\n\n"
        
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
        print(f"Email sent successfully to {RECIPIENT_EMAIL}")
    except Exception as e:
        print(f"Error sending email: {e}")


def main():
    print(f"Starting Campaign Alert Script (v4.0 - Python Only) at {datetime.datetime.now()}")
    client = get_sheets_client()
    result = fetch_spreadsheet_data(client)
    if result is None: return
    
    df, date_col, status_col = result
    alerts = analyze_data(df, date_col, status_col)
    
    subject, body = format_email(alerts)
    if subject:
        print(f"Found {len(alerts)} alerts. Sending email...")
        send_email(subject, body)
    else:
        print(body)

if __name__ == "__main__":
    main()
