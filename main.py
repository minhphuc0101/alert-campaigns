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
# 1. API Keys & Emails
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
SENDER_EMAIL = os.getenv("EMAIL_SENDER_ADDRESS")
SENDER_PASSWORD = os.getenv("EMAIL_SENDER_PASSWORD")
RECIPIENT_EMAIL = "phuc.tran@digimind.asia"

# 2. Google Sheets Info
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
TAB_NAME = os.getenv("GOOGLE_SHEET_TAB_NAME")

# 3. Settings
LOOKBACK_DAYS = 7


def get_sheets_client():
    """Authenticates and returns a gspread client."""
    # Define the scopes required by the API
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly"
    ]
    
    # We assume the user has a service account JSON downloaed as 'service_account.json'
    # in the same directory as this script.
    try:
        credentials = Credentials.from_service_account_file(
            "service_account.json", scopes=scopes
        )
        client = gspread.authorize(credentials)
        return client
    except FileNotFoundError:
        print("Error: 'service_account.json' not found.")
        print("Please create a Google Cloud Service Account, download the JSON key, name it 'service_account.json', and place it in this folder.")
        print("Also remember to share your Google Sheet with the service account email address.")
        exit(1)


def fetch_recent_data(client):
    """Fetches data from the Google Sheet and filters for the last LOOKBACK_DAYS."""
    try:
        sheet = client.open_by_key(SHEET_ID).worksheet(TAB_NAME)
    except Exception as e:
        print(f"Error opening Google Sheet (ID: {SHEET_ID}, Tab: {TAB_NAME}): {e}")
        exit(1)

    # Get all records as a list of dictionaries
    # We assume the first row has headers: 'Date', 'Campaign name', 'Amount spent'
    data = sheet.get_all_records()
    df = pd.DataFrame(data)

    if df.empty:
        print("The sheet is empty.")
        return None

    # Standardize column names (lowercase and strip spaces for easier matching)
    df.columns = [str(c).strip().lower() for c in df.columns]

    # Find the date column
    date_col = None
    # Common variations of "date"
    date_keywords = ['date', 'day', 'time', 'period']
    
    print(f"DEBUG: Processing headers: {df.columns.tolist()}")
    
    for col in df.columns:
        col_clean = str(col).strip().lower()
        if any(keyword in col_clean for keyword in date_keywords):
            date_col = col
            print(f"DEBUG: Selected '{date_col}' as the date column.")
            break
            
    if not date_col:
        print("Error: Could not determine which column contains the Date.")
        print(f"Found columns: {df.columns.tolist()}")
        print(f"Searched for keywords: {date_keywords}")
        return None

    # Convert date column to datetime objects
    try:
        # Infer datetime format automatically
        df[date_col] = pd.to_datetime(df[date_col], format='mixed', dayfirst=False)
    except Exception as e:
        print(f"Error parsing dates in column '{date_col}': {e}")
        return None

    # Calculate the cutoff date (7 days ago)
    cutoff_date = datetime.datetime.now() - timedelta(days=LOOKBACK_DAYS)
    
    # Filter for the last 7 days
    recent_df = df[df[date_col] >= cutoff_date]
    
    # Sort by date
    recent_df = recent_df.sort_values(by=date_col, ascending=False)
    
    return recent_df


def analyze_with_gemini(data_str):
    """Sends the data to Gemini and asks for analysis on high-spending campaigns."""
    if not GEMINI_API_KEY:
        print("Error: GEMINI_API_KEY environment variable not set.")
        return None

    client = genai.Client(api_key=GEMINI_API_KEY)
    
    prompt = f"""
You are an expert digital marketing analyst. 
Review the following Facebook ad spend and performance data for the past {LOOKBACK_DAYS} days.
The data includes: Date, Campaign name, Campaign Status, Amount spent, Campaign Spend Cap, Post Engagement, Reach, Impressions, and 3-Second Video Views.

1. EXCLUSION: COMPLETELY IGNORE any campaigns that are for "App Installs". Do not analyze or report on them.
2. AGGREGATE DATA: For each unique Campaign Name, sum up all "Amount spent", "Post Engagement", "Reach", "Impressions", and "3-Second Video Views" across the provided data.
3. EXTRACT KPIs FROM CAMPAIGN NAMES:
   - "X CPM" or "CPM_X" (e.g., "CPM_100"): Volume target = X * 1000 total Impressions.
   - "X CPE" or "CPE_X" (e.g., "3000 CPE"): Rate target = Cost Per Engagement <= X.
4. UNIT COST SPIKE DETECTION:
   - Identify the "current" unit cost (latest day with spend).
   - Calculate the "average unit cost" for that same campaign over the 3 days prior to the latest day.
   - ALERT if the current unit cost is >30% higher than the 3-day average unit cost for that campaign.
   - Use (Spent / Impressions * 1000) for CPM campaigns and (Spent / Engagement) for CPE campaigns.
5. IDENTIFY ISSUES:
   - Rate targets: Total Spent / Total Post Engagement > Target.
   - Volume targets: Total Impressions match/under target relative to spend.
   - Status check: Note the "Campaign Status" (Active, Paused, etc.).
6. ANALYZE RESULTS: Determine a short "Reason" (e.g., "Unit cost spike >30%", "Over achieved", "KPI target missed", "Status: Paused but high spend").

CRITICAL OUTPUT FORMAT:
You MUST format your response exactly like this template:

Subject: 🚨 Campaign Alert: [Number of campaigns] Campaigns Require Attention

Hi Team,

The following campaigns are currently exceeding their target KPIs or spending anomalies have been detected:

1. [Campaign Name] ([Campaign Status])
   - 📉 Issue: [e.g., Unit cost spiked by 45% compared to last 3 days avg; Target 100k Imp, at 80k]
   - 💰 Spent: [Total Amount Spent]
   - 📊 Metrics: [Relevant metrics]
   - 🏷️ Reason: [Short reason]

[Repeat for each flagged campaign]

---
Summary of Good/Normal Campaigns:
[Brief 1-line summary of other campaigns that are pacing well]

Please review your Ads Manager.
- AI Alert System

If spending looks completely normal, KPIs are being met, and no single campaign is dominating the budget unusually, simply output the exact sentence: "Spending is within normal parameters and no action is required today."

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
        print(f"Error calling Gemini API: {e}")
        return None


def send_email_alert(subject, body):
    """Sends an email using SMTP."""
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("Error: EMAIL_SENDER_ADDRESS or EMAIL_SENDER_PASSWORD not set.")
        return

    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        # Assuming Gmail SMTP. Change this if using a different provider (e.g., SendGrid, Outlook)
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        text = msg.as_string()
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, text)
        server.quit()
        print(f"Successfully sent alert email to {RECIPIENT_EMAIL}")
    except Exception as e:
        print(f"Error sending email: {e}")


def main():
    print(f"Starting Campaign Alert Script (v1.2 - Robust Logic) at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("1. Authenticating with Google Sheets...")
    client = get_sheets_client()
    
    print(f"2. Fetching data for the last {LOOKBACK_DAYS} days...")
    recent_df = fetch_recent_data(client)
    
    if recent_df is None or recent_df.empty:
        print("No recent data found. Exiting.")
        return
        
    print(f"Found {len(recent_df)} records.")
    
    # Convert dataframe to a string representation for the AI
    # We only take the relevant columns assuming they containt 'campaign' and 'amount' or 'spend'
    # For safety, let's just convert the whole dataframe to CSV string
    data_csv_str = recent_df.to_csv(index=False)
    
    print("3. Analyzing data with Google Gemini AI...")
    analysis_text = analyze_with_gemini(data_csv_str)
    
    if not analysis_text:
        print("Failed to get analysis from Gemini. Exiting.")
        return
        
    print("--- Gemini Analysis Summary ---")
    print(analysis_text)
    print("-------------------------------")
    
    # Check if we should send an email based on the AI's response
    # If the AI says everything is normal, we might skip the email or send a quiet summary
    if "no action is required today" in analysis_text.lower() and "normal" in analysis_text.lower():
        print("AI indicated spending is normal. Still sending a daily summary.")
        subject = f"Daily Campaign Spend Summary - {datetime.datetime.now().strftime('%Y-%m-%d')}"
    else:
        print("AI found actionable insights. Sending an Alert.")
        subject = f"ACTION REQUIRED: High Spend Campaign Alert - {datetime.datetime.now().strftime('%Y-%m-%d')}"
        
    print(f"4. Sending email to {RECIPIENT_EMAIL}...")
    send_email_alert(subject, analysis_text)
    
    print("Done!")

if __name__ == "__main__":
    main()
