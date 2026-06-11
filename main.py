import os
import smtplib
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime
from datetime import timedelta
import pandas as pd
from dotenv import load_dotenv

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.user import User
from facebook_business.exceptions import FacebookRequestError

# Load environment variables
load_dotenv()

# Configuration
SENDER_EMAIL = os.getenv("EMAIL_SENDER_ADDRESS")
SENDER_PASSWORD = os.getenv("EMAIL_SENDER_PASSWORD")
RECIPIENT_EMAIL = "phuc.tran@digimind.asia"
CC_EMAIL = "mediagroup@digimind.asia"

# Meta API Config
FB_ACCESS_TOKEN = os.getenv("FB_ACCESS_TOKEN")
FB_AD_ACCOUNT_ID = os.getenv("FB_AD_ACCOUNT_ID") # Optional fallback

def fetch_meta_ads_data(access_token):
    if not access_token:
        print("Error: Meta Access Token (FB_ACCESS_TOKEN) is not set.")
        return None
    
    FacebookAdsApi.init(access_token=access_token)
    me = User(fbid='me')
    
    try:
        my_accounts = me.get_ad_accounts(fields=['id', 'name'], params={'limit': 500})
    except FacebookRequestError as e:
        print(f"Error fetching ad accounts: {e}")
        return None

    target_accounts = []
    for acc in my_accounts:
        name = acc.get('name', '')
        if name.startswith('DIGIMIND_'):
            target_accounts.append(acc)
            
    if not target_accounts:
        print("No ad accounts found starting with 'DIGIMIND_'.")
        return None

    all_campaign_data = []
    
    today = datetime.datetime.now().date()
    fourteen_days_ago = today - timedelta(days=14)
    time_range_kpi = {
        'since': fourteen_days_ago.strftime('%Y-%m-%d'),
        'until': today.strftime('%Y-%m-%d')
    }

    four_days_ago = today - timedelta(days=4)
    time_range_daily = {
        'since': four_days_ago.strftime('%Y-%m-%d'),
        'until': today.strftime('%Y-%m-%d')
    }

    for acc in target_accounts:
        acc_id = acc['id']
        acc_name = acc.get('name', 'Unknown')
        print(f"Fetching data for account: {acc_name} ({acc_id})")
        account = AdAccount(acc_id)
        
        try:
            campaigns = account.get_campaigns(
                fields=['name', 'status', 'effective_status', 'spend_cap'],
                params={'filtering': '[{"field":"effective_status","operator":"IN","value":["ACTIVE","PAUSED"]}]', 'limit': 500}
            )
        except Exception as e:
            print(f"Error fetching campaigns for {acc_name}: {e}")
            continue
            
        if not campaigns:
            continue
            
        camp_dict = {c['id']: c for c in campaigns}
        
        try:
            insights_14d = account.get_insights(
                fields=['campaign_id', 'spend', 'reach', 'impressions', 'actions', 'video_p3c_video_views'],
                params={'level': 'campaign', 'time_range': time_range_kpi, 'limit': 500}
            )
        except Exception as e:
            print(f"Error fetching 14d insights for {acc_name}: {e}")
            insights_14d = []

        camp_14d_stats = {}
        for row in insights_14d:
            c_id = row.get('campaign_id')
            spend = float(row.get('spend', 0))
            reach = int(row.get('reach', 0))
            impressions = int(row.get('impressions', 0))
            
            engagement = 0
            video_views = float(row.get('video_p3c_video_views', 0))
            actions = row.get('actions', [])
            for action in actions:
                a_type = action.get('action_type')
                val = float(action.get('value', 0))
                if a_type == 'post_engagement':
                    engagement += val
                elif a_type == 'video_view':
                    video_views += val
                    
            camp_14d_stats[c_id] = {
                'total_spent': spend,
                'total_reach': reach,
                'total_impressions': impressions,
                'total_engagement': engagement,
                'total_video_views': video_views
            }

        try:
            insights_daily = account.get_insights(
                fields=['campaign_id', 'spend'],
                params={'level': 'campaign', 'time_range': time_range_daily, 'time_increment': 1, 'limit': 1000}
            )
        except Exception as e:
            print(f"Error fetching daily insights for {acc_name}: {e}")
            insights_daily = []

        camp_daily_spend = {}
        for row in insights_daily:
            c_id = row.get('campaign_id')
            date_start = row.get('date_start')
            spend = float(row.get('spend', 0))
            
            if c_id not in camp_daily_spend:
                camp_daily_spend[c_id] = {}
            camp_daily_spend[c_id][date_start] = spend

        for c_id, c in camp_dict.items():
            name = c.get('name', 'Unknown')
            if re.search(r'app install|lead|messenger', name, re.IGNORECASE):
                continue
                
            status = c.get('effective_status', c.get('status', 'UNKNOWN')).lower()
            spend_cap = float(c.get('spend_cap', 0))
            
            stats_14d = camp_14d_stats.get(c_id, {})
            total_spent = stats_14d.get('total_spent', 0)
            
            if total_spent < 2000000:
                continue
                
            row = {
                'ad_account_id': acc_id.replace('act_', ''),
                'ad_account_name': acc_name,
                'campaign_id': c_id,
                'campaign_name': name,
                'status': status,
                'spend_cap': spend_cap,
                'total_spent': total_spent,
                'total_reach': stats_14d.get('total_reach', 0),
                'total_impressions': stats_14d.get('total_impressions', 0),
                'total_engagement': stats_14d.get('total_engagement', 0),
                'total_video_views': stats_14d.get('total_video_views', 0),
                'daily_spend': camp_daily_spend.get(c_id, {})
            }
            all_campaign_data.append(row)
            
    return all_campaign_data

def extract_kpi_target(campaign_name):
    """Extracts numerical target from name (e.g. '3000 CPE' -> 3000, 'CPM_100' -> 100)."""
    # Look for numbers optionally before and after CPE/CPM
    cpe_match = re.search(r'(?:(\d+[,\d]*)[_\s]*)?CPE(?:[_\s]*(\d+[,\d]*))?', campaign_name, re.I)
    cpm_match = re.search(r'(?:(\d+[,\d]*)[_\s]*)?CPM(?:[_\s]*(\d+[,\d]*))?', campaign_name, re.I)
    
    def parse_num(val):
        if not val: return 0
        return int(val.replace(',', ''))

    if cpe_match:
        val = max(parse_num(cpe_match.group(1)), parse_num(cpe_match.group(2)))
        if val > 0: return 'CPE', val
    if cpm_match:
        val = max(parse_num(cpm_match.group(1)), parse_num(cpm_match.group(2)))
        # CPM is volume: X * 1000 impressions
        if val > 0: return 'CPM', val * 1000
        
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
            # Clean ID (V8.4: Robust cleaning - avoid floats for precision)
            acc_id_str = str(account_id).strip()
            if acc_id_str.endswith('.0'): acc_id_str = acc_id_str[:-2]
            
            if not acc_id_str or acc_id_str == 'nan' or acc_id_str == 'None': continue
            
            full_id = acc_id_str if acc_id_str.startswith('act_') else f"act_{acc_id_str}"
            print(f"Checking Account: {full_id} (V8.4 - Strict String Mode)...")
            
            try:
                account = AdAccount(full_id)
                
                # 1. Automatic Campaign ID Lookup (Safety Net)
                name_to_id_map = {}
                try:
                    camps = account.get_campaigns(fields=['name', 'id'], params={'filtering': '[{"field":"effective_status","operator":"IN","value":["ACTIVE","PAUSED"]}]'})
                    for c in camps: # Meta SDK iterates all pages
                        name_to_id_map[c.get('name')] = str(c.get('id'))
                except:
                    pass # Continue with Sheet IDs if lookup fails

                # 2. Fetch and count Rules across ALL pages (V7.9 - Paging & Global Detection)
                rules_cursor = account.get_ad_rules_library(
                    fields=['name', 'status', 'evaluation_spec', 'execution_spec'],
                    params={'limit': 250} # Optimize page size
                )
                
                target_campaign_ids = set()
                rule_count = 0
                enabled_count = 0
                for rule in rules_cursor: # SDK handles paging automatically
                    rule_count += 1
                    if rule.get('status') != 'ENABLED': continue
                    enabled_count += 1
                    
                    # 1. Action Check: Is it a "PAUSE" rule?
                    exec_spec = rule.get('execution_spec', {})
                    is_pause = exec_spec.get('execution_type') == 'PAUSE'
                    if not is_pause: continue
                    
                    # 2. Scope Check: Strict ID matching (V8.3)
                    eval_spec = rule.get('evaluation_spec', {})
                    filters = eval_spec.get('filters', [])
                    rule_name = rule.get('name', 'Unnamed Rule')
                    
                    rule_covers_ids = False
                    rule_ids = []
                    for f in filters:
                        if f.get('field') == 'campaign.id':
                            rule_covers_ids = True
                            vals = f.get('value')
                            if not isinstance(vals, list): vals = [vals]
                            rule_ids = [str(v) for v in vals]
                            target_campaign_ids.update(rule_ids)
                    
                    if rule_count <= 25: # V8.6: More verbose rule logging
                        if rule_covers_ids:
                             scope = f"ID-based, covers {len(rule_ids)}: {', '.join(rule_ids[:5])}{'...' if len(rule_ids)>5 else ''}"
                        else:
                             scope = "Other scope (Skipped)"
                        print(f"   [INFO] Found rule: {rule_name} ({scope})")
                
                print(f"DEBUG: Found {rule_count} rules ({enabled_count} Enabled). Audit covered {len(target_campaign_ids)} IDs.")
                
                rules_by_account[full_id] = {
                    'error': None,
                    'protected_ids': target_campaign_ids,
                    'meta_map': name_to_id_map
                }
                    
            except FacebookRequestError as e:
                # V8.4: Succinct logging for Permission Errors
                api_error = e.api_error_message()
                error_code = e.api_error_code()
                
                if error_code == 200:
                    err_msg = "Access Denied: Ad account owner has not granted ads_read/ads_management permission."
                else:
                    err_msg = f"Meta API Error ({error_code}): {api_error}"
                
                print(f"Error fetching rules for {full_id}: {err_msg}")
                rules_by_account[full_id] = {
                    'error': err_msg,
                    'protected_ids': set(),
                    'meta_map': {}
                }
            except Exception as e:
                err_msg = f"Unexpected Error: {str(e)[:100]}..." # Avoid huge error dumps
                print(f"Error fetching rules for {full_id}: {err_msg}")
                rules_by_account[full_id] = {
                    'error': err_msg,
                    'protected_ids': set(),
                    'meta_map': {}
                }
            
    except Exception as e:
        error_msg = f"Meta API Connection Error: {str(e)}"
        print(error_msg)
    
    return rules_by_account, error_msg


def fetch_meta_ad_creatives(access_token, ad_account_ids):
    """Fetches active ads to check for Advantage+ creative options."""
    if not access_token:
        return {}, "Meta Access Token (FB_ACCESS_TOKEN) is not set."

    enhancements_by_account = {}
    error_msg = None
    try:
        FacebookAdsApi.init(access_token=access_token)
        for account_id in ad_account_ids:
            acc_id_str = str(account_id).strip()
            if acc_id_str.endswith('.0'): acc_id_str = acc_id_str[:-2]
            if not acc_id_str or acc_id_str == 'nan' or acc_id_str == 'None': continue
            
            full_id = acc_id_str if acc_id_str.startswith('act_') else f"act_{acc_id_str}"
            
            try:
                account = AdAccount(full_id)
                ads = account.get_ads(
                    fields=['campaign_id', 'creative{degrees_of_freedom_spec}'],
                    params={'filtering': '[{"field":"effective_status","operator":"IN","value":["ACTIVE"]}]'}
                )
                
                campaign_enhancements = {}
                for ad in ads:
                    camp_id = ad.get('campaign_id')
                    creative = ad.get('creative', {})
                    dof = creative.get('degrees_of_freedom_spec', {})
                    features = dof.get('creative_features_spec', {})
                    
                    active_options = []
                    if features.get('standard_enhancements', {}).get('enroll_status') == 'OPT_IN':
                        active_options.append('Standard Enhancements')
                    if features.get('multi_advertiser_ad_display', {}).get('enroll_status') == 'OPT_IN':
                        active_options.append('Multi-advertiser Ads')
                    if features.get('text_optimizations', {}).get('enroll_status') == 'OPT_IN':
                        active_options.append('Text Optimizations')
                    
                    if active_options and camp_id:
                        if camp_id not in campaign_enhancements:
                            campaign_enhancements[camp_id] = set()
                        for opt in active_options:
                            campaign_enhancements[camp_id].add(opt)
                            
                enhancements_by_account[full_id] = campaign_enhancements
            except Exception as e:
                print(f"Error fetching creatives for {full_id}: {str(e)[:100]}")
    except Exception as e:
        error_msg = str(e)
    return enhancements_by_account, error_msg



def analyze_data(campaign_data):
    alerts = []
    
    unique_account_ids = list(set([row['ad_account_id'] for row in campaign_data]))
    
    meta_rules, meta_error = fetch_meta_automated_rules(FB_ACCESS_TOKEN, unique_account_ids)
    meta_creatives, _ = fetch_meta_ad_creatives(FB_ACCESS_TOKEN, unique_account_ids)
    
    missing_automation_alerts = []
    advantage_alerts = []
    skipped_audit_reason = meta_error
    
    today = datetime.datetime.now().date()
    yesterday_str = (today - timedelta(days=1)).strftime('%Y-%m-%d')
    prior_3_dates = [(today - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(2, 5)]

    for row in campaign_data:
        campaign = row['campaign_name']
        status = row['status']
        total_spent = row['total_spent']
        total_engagement = row['total_engagement']
        total_impressions = row['total_impressions']
        total_reach = row['total_reach']
        total_video_views = row['total_video_views']
        
        kpi_type, kpi_target = extract_kpi_target(campaign)
        if kpi_type and kpi_target:
            if kpi_type == 'CPE':
                if total_engagement > kpi_target:
                    alerts.append({
                        'campaign': campaign,
                        'status': status,
                        'issue': f"Volume Target reached (Target: {kpi_target:,} engagement, Actual 14d: {total_engagement:,.0f})",
                        'spent_line': f"{total_spent:,.0f} (14d)",
                        'metrics': f"Total Post Engagement: {total_engagement:,.0f}, Reach: {total_reach:,.0f}, Impressions: {total_impressions:,.0f}",
                        'reason': "Engagement target achieved"
                    })
            elif kpi_type == 'CPM':
                if total_impressions > kpi_target:
                    alerts.append({
                        'campaign': campaign,
                        'status': status,
                        'issue': f"Volume Target reached (Target: {kpi_target:,.0f} impressions, Actual 14d: {total_impressions:,.0f})",
                        'spent_line': f"{total_spent:,.0f} (14d)",
                        'metrics': f"Total Impressions: {total_impressions:,.0f}, Reach: {total_reach:,.0f}",
                        'reason': "Impression target achieved"
                    })

        spend_cap = row['spend_cap']
        if spend_cap > 0 and total_spent > spend_cap:
             alerts.append({
                'campaign': campaign,
                'status': status,
                'issue': f"Spending exceeds cap. Spent: {total_spent:,.0f} (Cap: {spend_cap:,.0f})",
                'spent_line': f"{total_spent:,.0f} (Cap: {spend_cap:,.0f})",
                'metrics': f"Reach: {total_reach:,.0f}, Impressions: {total_impressions:,.0f}, Post Engagement: {total_engagement:,.0f}",
                'reason': "Spending exceeds cap"
            })

        daily_spends = row['daily_spend']
        yesterday_spend = daily_spends.get(yesterday_str, 0)
        prior_spends = [daily_spends.get(d, 0) for d in prior_3_dates]
        
        avg_prior_spend = sum(prior_spends) / len(prior_spends) if prior_spends else 0
        
        if avg_prior_spend >= 1000000:
            percent_increase = (yesterday_spend - avg_prior_spend) / avg_prior_spend if avg_prior_spend > 0 else 0
            if percent_increase > 0.30:
                alerts.append({
                    'campaign': campaign,
                    'status': status,
                    'issue': f"Yesterday spend ({yesterday_spend:,.0f}) is {percent_increase*100:.1f}% higher than 3-day avg ({avg_prior_spend:,.0f})",
                    'spent_line': f"Yesterday: {yesterday_spend:,.0f} (Avg: {avg_prior_spend:,.0f})",
                    'metrics': f"Date: {yesterday_str}",
                    'reason': "High spend anomaly"
                })

        if status == 'active':
            acc_id_str = str(row['ad_account_id'])
            acc_name = row['ad_account_name']
            camp_id = str(row['campaign_id'])
            
            full_acc_id = f"act_{acc_id_str}" if not acc_id_str.startswith('act_') else acc_id_str
            if full_acc_id in meta_rules:
                acc_data = meta_rules[full_acc_id]
                if acc_data.get('error'):
                    missing_automation_alerts.append({
                        'campaign': campaign,
                        'acc_name': acc_name,
                        'issue': f"Meta Audit Skipped: {acc_data['error']}",
                        'reason': "Missing Meta Automation"
                    })
                else:
                    protected_ids = acc_data.get('protected_ids', set())
                    is_covered = camp_id in protected_ids
                    
                    if not is_covered:
                         missing_automation_alerts.append({
                            'campaign': campaign,
                            'acc_name': acc_name,
                            'issue': "No active 'Pause' rule found for this campaign ID",
                            'reason': "Missing Meta Automation"
                        })
                    
                    if camp_id and full_acc_id in meta_creatives:
                        enabled_options = meta_creatives[full_acc_id].get(camp_id)
                        if enabled_options:
                            advantage_alerts.append({
                                'campaign': campaign,
                                'options': list(enabled_options)
                            })
            else:
                if not meta_error:
                    missing_automation_alerts.append({
                        'campaign': campaign,
                        'status': status,
                        'issue': "Audit skipped (Could not connect to this Ad Account)",
                        'spent_line': f"Account: {full_acc_id}",
                        'metrics': "Check your FB_ACCESS_TOKEN permissions",
                        'reason': "Missing Meta Automation"
                    })
                    
    kpi_alerts = [a for a in alerts if a['reason'] in ["Engagement target achieved", "Impression target achieved", "Spending exceeds cap"]]
    anomaly_alerts = [a for a in alerts if a['reason'] == "High spend anomaly"]
    
    return {
        'kpi': kpi_alerts,
        'anomaly': anomaly_alerts,
        'missing_automation': missing_automation_alerts,
        'advantage': advantage_alerts,
        'audit_error': skipped_audit_reason
    }


def format_email(alert_groups):
    has_alerts = any(k != 'audit_error' and v for k, v in alert_groups.items())
    if not has_alerts and not alert_groups.get('audit_error'):
        return None, "Spending is within normal parameters and no action is required today."
        
    subject = f"🚨 Action Required: Campaign Alert [V8.8] - {datetime.datetime.now().strftime('%Y-%m-%d')}"
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

    # Section 3: Missing Automation (Concise V7.1)
    if alert_groups['missing_automation']:
        body += "🛡️ SECTION 3: MISSING AUTOMATION RULES (Meta)\n"
        body += "========================================\n"
        body += "The following campaigns are ACTIVE but have no automated 'Pause' rules found in Meta:\n\n"
        for i, alert in enumerate(alert_groups['missing_automation'], 1):
            body += f"{i}. {alert['campaign']}, {alert['acc_name']}\n"
        body += "\n"
        
    # Section 4: Advantage+ Enabled
    if 'advantage' in alert_groups:
        body += "✨ SECTION 4: ADVANTAGE+ CREATIVE ENABLED\n"
        body += "========================================\n"
        if alert_groups['advantage']:
            body += "The following active campaigns have Advantage+ options enabled:\n\n"
            for i, alert in enumerate(alert_groups['advantage'], 1):
                opts_str = ", ".join(alert['options'])
                body += f"{i}. {alert['campaign']}\n"
                body += f"   - Enabled features: {opts_str}\n\n"
        else:
            body += "No active campaigns currently have Advantage+ enabled.\n\n"
            
    body += "---\nPlease review your Ads Manager.\n- Alert System (V8.9)"
    return subject, body


def send_email(subject, body):
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        return print("Error: Email credentials missing.")
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    # msg['Cc'] = CC_EMAIL
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        recipients = [RECIPIENT_EMAIL] # CC_EMAIL removed for testing
        server.sendmail(SENDER_EMAIL, recipients, msg.as_string())
        server.quit()
        print(f"Email sent successfully to {RECIPIENT_EMAIL}")
    except Exception as e:
        print(f"Error sending email: {e}")



def main():
    print(f"Starting Campaign Alert Script (Direct Meta API) at {datetime.datetime.now()}")
    
    campaign_data = fetch_meta_ads_data(FB_ACCESS_TOKEN)
    if not campaign_data:
        print("No campaign data found or error occurred. Exiting.")
        return
        
    alert_groups = analyze_data(campaign_data)
    
    subject, body = format_email(alert_groups)
    if subject:
        total_count = len(alert_groups['kpi']) + len(alert_groups['anomaly']) + len(alert_groups['missing_automation'])
        print(f"Found {total_count} alerts. Sending email...")
        send_email(subject, body)
    else:
        print(body)

if __name__ == "__main__":
    main()
