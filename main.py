import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime
from datetime import timedelta
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from google import genai
from google.genai import types
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
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

    df.columns = [str(c).strip().lower() for c in df.columns]

    date_keywords = ['date', 'day']
    date_col = next((col for col in df.columns if any(kw in col for kw in date_keywords)), None)
            
    if not date_col:
        print(f"Error: Could not find a date column. Headers: {df.columns.tolist()}")
        return None

    df[date_col] = pd.to_datetime(df[date_col], format='mixed', dayfirst=False)
    cutoff_date = pd.Timestamp.now().normalize() - timedelta(days=LOOKBACK_DAYS)
    recent_df = df[df[date_col] >= cutoff_date]
    recent_df = recent_df.sort_values(by=date_col, ascending=False)
    
    return recent_df


def analyze_with_gemini(data_str):
    """Sends the data to Gemini for analysis with strict marketer logic."""
    if not GEMINI_API_KEY:
        print("Error: GEMINI_API_KEY not set.")
        return None

    client = genai.Client(api_key=GEMINI_API_KEY)
    
    prompt = f"""
You are an expert digital marketing analyst. 
Review the following Facebook ad spend data for the past {LOOKBACK_DAYS} days.
The data includes: Date, Campaign name, Campaign Status, Amount spent, Campaign Spend Cap, Post Engagement, Reach, Impressions, and 3-Second Video Views.

CRITICAL INSTRUCTIONS:
1. EXCLUSIONS: COMPLETELY IGNORE any campaigns for "App Installs" or "Leads" / "Lead Generation".
2. KPI EXTRACTION: Only check KPI targets (CPE or CPM) if the Campaign Name contains an explicit number (e.g. "3000 CPE", "CPM_100").
3. LOGIC A (KPIs):
   - CPE Check: If (Sum of Spent / Sum of Engagement) > (Target in name * 1.05), alert.
   - CPM Check: If (Sum of Impressions) < (Target Volume * 0.95), alert. (Note: "CPM_100" = 100,000 Impressions).
   - UNDEFINED CASE: If Spent > 0 but Engagement or Impressions is 0, alert as "Actual [KPI] is undefined (no results for [Amount] spent)".
4. LOGIC B (SPEND CAP): Alert if the total sum of spent across 7 days > the "campaign spend cap" value.
5. LOGIC C (ANOMALIES): Alert if "Yesterday" (latest date in data) spend is >30% higher than the average spend of the 3 days immediately preceding it.

CRITICAL OUTPUT FORMAT:
You MUST format each flagged campaign exactly like this:

[No]. [Campaign Name]
   - 📉 Issue: [Specifically explain if it's a KPI miss, Spend Cap miss, or Anomaly]
   - 💰 Spent: [Total Spent] (Cap: [Target Spend Cap or N/A])
   - 📊 Metrics: Reach: [Value], Impressions: [Value], Post Engagement: [Value], Video Views: [Value]
   - campaign status: [active or paused]

---
If nothing requires attention, only output: "Spending is within normal parameters."

Here is the data:
{data_str}
"""
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
        )
        return response.text
    except Exception as e:
        print(f"Error calling Gemini: {e}")
        return None


def send_email_alert(subject, body):
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
    print(f"Starting Campaign Alert Script (v3.0 - AI + Strict Logic) at {datetime.datetime.now()}")
    client = get_sheets_client()
    recent_df = fetch_recent_data(client)
    
    if recent_df is None or recent_df.empty:
        print("No recent data found. Exiting.")
        return
        
    print(f"Found {len(recent_df)} records. Analyzing...")
    data_csv = recent_df.to_csv(index=False)
    analysis_text = analyze_with_gemini(data_csv)
    
    if not analysis_text or "within normal parameters" in analysis_text.lower():
        print("Everything looks normal. No alert sent.")
        return

    print("Alerts found! Sending summary...")
    subject = f"🚨 Action Required: Campaign Spend Alert - {datetime.datetime.now().strftime('%Y-%m-%d')}"
    send_email_alert(subject, analysis_text)
    print("Done!")

if __name__ == "__main__":
    main()
