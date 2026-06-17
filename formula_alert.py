import os
import re
import gspread
import traceback
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import sys
import io
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

load_dotenv()

def get_sheets_client():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds_json_path = os.getenv("GOOGLE_CREDS_PATH", r'd:\antigravity\fb-ad-assistant-aca76f8d8ea2.json')
    if os.path.exists(creds_json_path):
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json_path, scope)
    else:
        import json
        creds_content = os.getenv("GOOGLE_CREDENTIALS")
        creds_dict = json.loads(creds_content)
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def col_num_to_letter(n):
    # n is 0-indexed. 0 -> A, 1 -> B, 25 -> Z, 26 -> AA
    string = ""
    n += 1
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def check_all_nested_formulas():
    client = get_sheets_client()
    main_url = 'https://docs.google.com/spreadsheets/d/1GCa7ODqOs_5Ur_pgDOKIsUHVNqqlYIUn386tDTEavP8/edit#gid=0'
    try:
        main_sheet = client.open_by_url(main_url).worksheet("Master data")
        all_records = main_sheet.get_all_records(expected_headers=['Year', 'PIC', 'Client name', 'Job ID', 'Campaign name', 'Status', 'Sheet URL', 'Sheet name', 'Budget', 'Plan %', 'Cost', 'Revenue', 'Act. %', 'Gross Profit', 'Achieved', 'Progress', '', 'Start', 'End', 'Time spent', 'WHT included', 'Color code'])
    except Exception as e:
        print(f"Error fetching main sheet: {e}")
        return [f"❌ Không thể truy cập file chính: {e}"]
        
    alerts = []
    
    for row in all_records:
        year = str(row.get('Year', '')).strip()
        if year != '2026':
            continue
            
        # Dùng Job ID làm tên hiển thị, nếu không có thì fallback về Campaign name
        job_id = str(row.get('Job ID', '')).strip()
        camp_name = str(row.get('Campaign name', '')).strip()
        campaign = job_id if job_id else camp_name
        
        url = row.get('Sheet URL', '')
        sheet_name = row.get('Sheet name', '')
        
        if not url or "docs.google.com" not in url:
            continue
            
        time.sleep(3) # Base sleep 3 seconds (Google limit is 60 req/min, each sheet takes ~3 reqs = 20 sheets/min max)
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                doc = client.open_by_url(url)
                try:
                    ws = doc.worksheet(sheet_name)
                except Exception:
                    ws = doc.worksheets()[0]
                    
                formulas = ws.get_all_values(value_render_option='FORMULA')
                values = ws.get_all_values(value_render_option='FORMATTED_VALUE')
                
                # Find headers
                header_row_idx = -1
                for i, r in enumerate(values):
                    if 'Hạng mục' in r and 'Cost' in r:
                        header_row_idx = i
                        break
                        
                if header_row_idx == -1:
                    break # exit retry loop
                    
                headers = values[header_row_idx]
                hang_muc_idx = headers.index('Hạng mục')
                cost_idx = headers.index('Cost')
                cost_col_letter = col_num_to_letter(cost_idx)
                
                first_data_row = header_row_idx + 2
                last_data_row = -1
                
                # Parse data rows
                for i in range(header_row_idx + 1, len(values)):
                    val_row = values[i]
                    form_row = formulas[i]
                    
                    hang_muc = str(val_row[hang_muc_idx]).strip() if len(val_row) > hang_muc_idx else ""
                    cost_form = str(form_row[cost_idx]).strip().lower() if len(form_row) > cost_idx else ""
                    
                    if hang_muc.lower() == 'total' or hang_muc == '':
                        last_data_row = i + 1
                        if hang_muc.lower() == 'total':
                            expected_sum = f"sum({cost_col_letter.lower()}{first_data_row}:{cost_col_letter.lower()}{last_data_row-1})"
                            if expected_sum not in cost_form.replace("$", ""):
                                alerts.append(f"⚠️ **{campaign}**: Hàm SUM ở dòng Total (Cost) bị sai dải dữ liệu! (Công thức hiện tại: `{cost_form}`, Đáng lý phải là: `{expected_sum}`)")
                        break
                        
                    hm_lower = hang_muc.lower()
                    if "fb" in hm_lower or "facebook" in hm_lower:
                        if "*1.05" not in cost_form.replace(" ", ""):
                            alerts.append(f"❌ **{campaign}**: Hạng mục `{hang_muc}` thiếu thuế 5% FB! (Công thức hiện tại: `{cost_form}`)")
                    elif "adx" in hm_lower or "zalo" in hm_lower:
                        if "/1.08" not in cost_form.replace(" ", ""):
                            alerts.append(f"❌ **{campaign}**: Hạng mục `{hang_muc}` thiếu trừ VAT 8% ADX/Zalo! (Công thức hiện tại: `{cost_form}`)")
                    elif "gg" in hm_lower or "google" in hm_lower:
                        if "1.05" in cost_form or "1.08" in cost_form:
                            alerts.append(f"❌ **{campaign}**: Hạng mục `{hang_muc}` (GG) không được tính thuế, nhưng công thức có vẻ sai! (Công thức hiện tại: `{cost_form}`)")
                
                # Success, break retry loop
                break
                
            except Exception as e:
                if "429" in str(e) or "Quota" in str(e):
                    if attempt < max_retries - 1:
                        time.sleep(15) # Wait 15s to cool down before retry
                    else:
                        alerts.append(f"⏳ **{campaign}**: Quá giới hạn API của Google (Rate limit). Đã thử lại 3 lần nhưng thất bại!")
                elif "Permission" in str(e):
                    alerts.append(f"🔏 **{campaign}**: Bot không có quyền truy cập file PNL! Hãy share file cho bot.")
                    break # No point retrying permission error
                else:
                    break # Ignore other errors
                
    return alerts

def send_email(subject, body):
    SENDER_EMAIL = os.getenv("EMAIL_SENDER_ADDRESS")
    SENDER_PASSWORD = os.getenv("EMAIL_SENDER_PASSWORD")
    RECIPIENT_EMAIL = "phuc.tran@digimind.asia"
    
    if not SENDER_EMAIL or not SENDER_PASSWORD:
        print("Error: Email credentials missing. Please set EMAIL_SENDER_ADDRESS and EMAIL_SENDER_PASSWORD.")
        return
        
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain', 'utf-8'))
    
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENT_EMAIL, msg.as_string())
        server.quit()
        print(f"Email sent successfully to {RECIPIENT_EMAIL}")
    except Exception as e:
        print(f"Error sending email: {e}")

if __name__ == "__main__":
    print("Running nested formula validator and sending email...")
    alerts = check_all_nested_formulas()
    
    import datetime
    subject = f"📊 PNL Formula Audit Report - {datetime.datetime.now().strftime('%Y-%m-%d')}"
    
    if alerts:
        body = "Hi Team,\n\nThe formula audit has detected the following errors in the 2026 PNL sheets:\n\n"
        for a in alerts:
            body += f"- {a}\n\n"
        body += "Please review and correct these formulas.\n\n- Automated Alert System"
    else:
        body = "Hi Team,\n\nAll formulas (Cost and SUM) across the 2026 PNL sheets are correct!\n\n- Automated Alert System"
        
    send_email(subject, body)
