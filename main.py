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
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# Load environment variables
load_dotenv()

# Configuration
SENDER_EMAIL = os.getenv("EMAIL_SENDER_ADDRESS")
SENDER_PASSWORD = os.getenv("EMAIL_SENDER_PASSWORD")
RECIPIENT_EMAIL = "phuc.tran@digimind.asia"

# Google Sheets Info
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
TAB_NAME = os.getenv("GOOGLE_SHEET_TAB_NAME")

# Meta API Config
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
FB_AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID") # Optional fallback


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

    # 1. Standardize and Deduplicate Column Names
    raw_columns = [str(c).strip().lower() for c in df.columns]
    new_columns = []
    seen = {}
    for col in raw_columns:
        if col in seen:
            seen[col] += 1
            new_columns.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_columns.append(col)
    df.columns = new_columns

    # 2. Find Best Matching Columns via Keywords
    def find_col(keywords):
        for col in df.columns:
            if any(kw in col for kw in keywords):
                return col
        return None

    date_col = find_col(['date', 'day'])
    status_col = find_col(['status', 'state'])
    ad_account_col = find_col(['ad account id', 'account id', 'account_id'])
    campaign_id_col = find_col(['campaign id', 'id', 'campaign_id'])
    
    # Metrics Mapping
    metric_map = {
        'spent': find_col(['amount spent', 'spent', 'cost']),
        'engagement': find_col(['post engagement', 'engagement', 'interaction']),
        'impressions': find_col(['impressions']),
        'reach': find_col(['reach']),
        'video_views': find_col(['3-second video views', 'video views', 'views'])
    }
            
    if not date_col or not metric_map['spent']:
        print(f"Error: Could not find required columns (Date/Spent). Headers: {df.columns.tolist()}")
        return None

    # 3. Aggressive Metric Cleaning
    for key, col in metric_map.items():
        if col:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

    # 4. Convert Date and Sort
    df[date_col] = pd.to_datetime(df[date_col], format='mixed', dayfirst=False)
    df = df.sort_values(by=date_col, ascending=False)
    
    # Print mappings for debugging
    print(f"--- Column Mappings [V6.0] ---")
    print(f"Date: {date_col}")
    print(f"Status: {status_col}")
    print(f"Ad Account ID Map: {ad_account_col}")
    print(f"Campaign ID Map: {campaign_id_col}")
    for k, v in metric_map.items():
        print(f"{k.capitalize()}: {v}")
    print(f"-----------------------------")
    
    return df, date_col, status_col, metric_map, ad_account_col, campaign_id_col


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


def fetch_meta_automated_rules(access_token, ad_account_ids):
    """Fetches all automated rules for the given account IDs from Meta API."""
    if not access_token:
        return {}, "Meta Access Token (FB_ACCESS_TOKEN) is not set."

    rules_by_account = {}
    error_msg = None
    try:
        FacebookAdsApi.init(access_token=access_token)
        for account_id in ad_account_ids:
            # Flatten if it's a numpy array or single value
            acc_id_str = str(account_id).strip()
            if not acc_id_str or acc_id_str == 'nan' or acc_id_str == 'None': continue
            
            full_id = acc_id_str if acc_id_str.startswith('act_') else f"act_{acc_id_str}"
            print(f"Fetching Meta rules for account: {full_id}...")
            
            try:
                account = AdAccount(full_id)
                rules = account.get_ads_api_rules(fields=['name', 'status', 'evaluation_spec', 'execution_spec'])
                
                target_campaign_ids = set()
                for rule in rules:
                    if rule.get('status') != 'ENABLED': continue
                    
                    exec_spec = rule.get('execution_spec', {})
                    if exec_spec.get('execution_type') not in ['PAUSE', 'TURN_OFF_CAMPAIGN', 'TURN_OFF_ADGROUP']:
                        continue
                    
                    eval_spec = rule.get('evaluation_spec', {})
                    filters = eval_spec.get('filters', [])
                    for f in filters:
                        if f.get('field') == 'campaign.id':
                            vals = f.get('value')
                            if isinstance(vals, list):
                                target_campaign_ids.update([str(v) for v in vals])
                            else:
                                target_campaign_ids.add(str(vals))
                
                rules_by_account[full_id] = target_campaign_ids
            except Exception as e:
                print(f"Error fetching rules for {full_id}: {e}")
                rules_by_account[full_id] = f"Error: {str(e)}"
            
    except Exception as e:
        error_msg = f"Meta API Connection Error: {str(e)}"
        print(error_msg)
    
    return rules_by_account, error_msg


def analyze_data(df, date_col, status_col, metric_map, ad_account_col, campaign_id_col):
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
    
    # Primary metrics from mapping
    spent_col = metric_map['spent']
    eng_col = metric_map['engagement']
    imp_col = metric_map['impressions']
    reach_col = metric_map['reach']
    views_col = metric_map['video_views']

    # Meta Automation Check (V6.2)
    unique_account_ids = df[ad_account_col].dropna().unique() if ad_account_col else []
    if len(unique_account_ids) == 0 and FB_AD_ACCOUNT_ID:
        unique_account_ids = [FB_AD_ACCOUNT_ID]
        
    meta_rules, meta_error = fetch_meta_automated_rules(FB_ACCESS_TOKEN, [str(i) for i in unique_account_ids])
    
    missing_automation_alerts = []
    skipped_audit_reason = meta_error

    for campaign in campaigns:
        campaign_df = df[df['campaign name'] == campaign]
        latest_row = campaign_df.iloc[0]
        
        # Calculate sums (Metrics are already cleaned to numeric in fetch)
        total_spent = campaign_df[spent_col].sum() if spent_col else 0
        
        # --- NEW V5.2: Threshold Check ---
        if total_spent < 2000000:
            continue
            
        # Determine status
        if status_col:
             status = str(latest_row.get(status_col, 'unknown')).strip()
        else:
             status = latest_row.get('campaign status', 'unknown')
        
        status = status.lower() if isinstance(status, str) else 'unknown'
        
        total_engagement = campaign_df[eng_col].sum() if eng_col else 0
        total_impressions = campaign_df[imp_col].sum() if imp_col else 0
        total_reach = campaign_df[reach_col].sum() if reach_col else 0
        total_video_views = campaign_df[views_col].sum() if views_col else 0
        
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
            yesterday_spend = yesterday_data[spent_col].sum() if spent_col else 0
            avg_prior_spend = prior_data[spent_col].sum() / len(prior_3_dates) if spent_col else 0
            
            if avg_prior_spend >= 1000000: # NEW V5.3: Only alert if 3-day avg spend >= 1M
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

        # 3. Meta Automation Rule Audit (V6.2)
        if status == 'active':
            acc_id = str(latest_row.get(ad_account_col, '')) if ad_account_col else FB_AD_ACCOUNT_ID
            camp_id = str(latest_row.get(campaign_id_col, '')) if campaign_id_col else ''
            
            acc_id_str = str(acc_id).strip()
            if not acc_id_str or acc_id_str == 'nan' or acc_id_str == 'None':
                missing_automation_alerts.append({
                    'campaign': campaign,
                    'status': status,
                    'issue': "Missing Ad Account ID in spreadsheet",
                    'spent_line': "Cannot check rules without Account ID",
                    'metrics': "High Risk",
                    'reason': "Missing Meta Automation"
                })
            else:
                full_acc_id = acc_id_str if acc_id_str.startswith('act_') else f"act_{acc_id_str}"
                if full_acc_id in meta_rules:
                    rules_data = meta_rules[full_acc_id]
                    if isinstance(rules_data, str) and rules_data.startswith('Error:'):
                        missing_automation_alerts.append({
                            'campaign': campaign,
                            'status': status,
                            'issue': f"API Audit Failed: {rules_data}",
                            'spent_line': f"Account: {full_acc_id}",
                            'metrics': "Audit Skipped due to API error",
                            'reason': "Missing Meta Automation"
                        })
                    else:
                        is_covered = False
                        if camp_id and str(camp_id) in rules_data:
                            is_covered = True
                        elif not camp_id and len(rules_data) > 0:
                            is_covered = True 
                        
                        if not is_covered:
                             missing_automation_alerts.append({
                                'campaign': campaign,
                                'status': status,
                                'issue': f"No active 'Pause' rule found for this campaign ID in Meta",
                                'spent_line': f"(Campaign ID: {camp_id if camp_id else 'Missing in Sheet'})",
                                'metrics': "High Risk: If target hit, script will alert but won't STOP in Meta!",
                                'reason': "Missing Meta Automation"
                            })
                else:
                    # Not in meta_rules but we had a token? maybe skip reason was missed
                    if not meta_error:
                        missing_automation_alerts.append({
                            'campaign': campaign,
                            'status': status,
                            'issue': "Audit skipped (Could not connect to this Ad Account)",
                            'spent_line': f"Account: {full_acc_id}",
                            'metrics': "Check your FB_ACCESS_TOKEN permissions",
                            'reason': "Missing Meta Automation"
                        })

    # Categorize alerts into Sections (V6.0)
    kpi_alerts = [a for a in alerts if a['reason'] in ["Engagement target achieved", "Impression target achieved", "Spending exceeds cap"]]
    anomaly_alerts = [a for a in alerts if a['reason'] == "High spend anomaly"]
    
    return {
        'kpi': kpi_alerts,
        'anomaly': anomaly_alerts,
        'missing_automation': missing_automation_alerts,
        'audit_error': skipped_audit_reason
    }


def format_email(alert_groups):
    has_alerts = any(k != 'audit_error' and v for k, v in alert_groups.items())
    if not has_alerts and not alert_groups.get('audit_error'):
        return None, "Spending is within normal parameters and no action is required today."
        
    subject = f"🚨 Action Required: Campaign Alert [V6.2 - AUDIT STATUS] - {datetime.datetime.now().strftime('%Y-%m-%d')}"
    body = "Hi Team,\n\nThe following campaigns require attention based on their performance and spending patterns:\n\n"
    
    if alert_groups.get('audit_error'):
        body += f"⚠️ META AUDIT SKIPPED: {alert_groups['audit_error']}\n"
        body += "Please check your FB_ACCESS_TOKEN and Ad Account IDs.\n\n"
    elif alert_groups['missing_automation']:
        body += "🚨 CRITICAL: Some active campaigns are missing Meta Automated Rules. They will NOT stop automatically!\n\n"
    
    # Section 1: KPI Achievement
    if alert_groups['kpi']:
        body += "🔴 SECTION 1: OVER-ACHIEVED VOLUME KPI\n"
        body += "========================================\n"
        for i, alert in enumerate(alert_groups['kpi'], 1):
            body += f"{i}. {alert['campaign']}\n"
            body += f"   - ❗ Issue: {alert['issue']}\n"
            body += f"   - 💰 Spent: {alert['spent_line']}\n"
            body += f"   - 📊 Metrics: {alert['metrics']}\n"
            body += f"   - 🚦 Campaign Status: {alert['status']}\n\n"
    
    # Section 2: Spend Anomalies
    if alert_groups['anomaly']:
        body += "⚠️ SECTION 2: HIGH SPENDING YESTERDAY\n"
        body += "========================================\n"
        for i, alert in enumerate(alert_groups['anomaly'], 1):
            body += f"{i}. {alert['campaign']}\n"
            body += f"   - ❗ Issue: {alert['issue']}\n"
            body += f"   - 💰 Spent: {alert['spent_line']}\n"
            body += f"   - 📊 Metrics: {alert['metrics']}\n"
            body += f"   - 🚦 Campaign Status: {alert['status']}\n\n"

    # Section 3: Missing Automation
    if alert_groups['missing_automation']:
        body += "🛡️ SECTION 3: MISSING AUTOMATION RULES (Meta)\n"
        body += "========================================\n"
        body += "The following campaigns are ACTIVE but have no automated 'Pause' rules in Meta. If they hit their KPI, they won't stop automatically!\n\n"
        for i, alert in enumerate(alert_groups['missing_automation'], 1):
            body += f"{i}. {alert['campaign']}\n"
            body += f"   - ❗ Issue: {alert['issue']}\n"
            body += f"   - 💰 Detail: {alert['spent_line']}\n"
            body += f"   - 🚦 Campaign Status: {alert['status']}\n\n"
            
    body += "---\nPlease review your Ads Manager.\n- Alert System (V6.2)"
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
    print(f"Starting Campaign Alert Script (v6.2 - Audit transparency) at {datetime.datetime.now()}")
    client = get_sheets_client()
    result = fetch_spreadsheet_data(client)
    if result is None: return
    
    df, date_col, status_col, metric_map, ad_account_col, campaign_id_col = result
    alert_groups = analyze_data(df, date_col, status_col, metric_map, ad_account_col, campaign_id_col)
    
    subject, body = format_email(alert_groups)
    if subject:
        total_count = len(alert_groups['kpi']) + len(alert_groups['anomaly']) + len(alert_groups['missing_automation'])
        print(f"Found {total_count} alerts. Sending email...")
        send_email(subject, body)
    else:
        print(body)

if __name__ == "__main__":
    main()
