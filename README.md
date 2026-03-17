# Setup Instructions for Campaign Alert Script

Welcome to your AI-powered Campaign Alert script! Follow these instructions to get it running.

## 1. Install Requirements

First, ensure you have Python installed. Then install the necessary libraries:

```bash
pip install -r requirements.txt
```

## 2. Set Up Environment Variables

1. Copy `.env.template` and rename it to `.env`.
2. Open `.env` and fill in your details:
   - **GEMINI_API_KEY**: Get this for free from [Google AI Studio](https://aistudio.google.com/app/apikey).
   - **EMAIL_SENDER_ADDRESS**: The email address the alerts will come *from* (e.g., your Gmail).
   - **EMAIL_SENDER_PASSWORD**: If using Gmail, you cannot use your normal password. You must generate an **App Password**. Go to your Google Account -> Security -> 2-Step Verification -> App passwords.
   - **GOOGLE_SHEET_ID**: The ID from your Google Sheet URL (the part between `/d/` and `/edit`).
   - **GOOGLE_SHEET_TAB_NAME**: The name of the tab containing the data (usually "Sheet1").

## 3. Set Up Google Sheets Access (Crucial Step)

The script needs a "Service Account" to read your private Google Sheet without popping up a browser login every day.

1. Go to the [Google Cloud Console](https://console.cloud.google.com/).
2. Create a new Project (or select an existing one).
3. Enable the **Google Sheets API** and **Google Drive API** for this project.
4. Go to **APIs & Services -> Credentials**.
5. Click **Create Credentials -> Service account**.
6. Give it a name and click Done.
7. Find the newly created service account in the list, click it, and go to the **Keys** tab.
8. Click **Add Key -> Create new key**, choose **JSON**, and click Create.
9. A `.json` file will download to your computer.
10. Rename this file to exactly **`service_account.json`** and place it in the same folder as `main.py`.
11. **IMPORTANT:** Open your Google Sheet in your browser, click "Share" in the top right, and share the sheet (as a Viewer) to the `client_email` address found inside your `service_account.json` file.

## 4. Run the Script

Run the script manually to test it:

```bash
python main.py
```

## 5. Automating the Script with GitHub Actions (CRON)

We have included a GitHub Actions workflow that will run this script automatically every day. 

To set this up, you need to add your `.env` variables and Service Account JSON into your GitHub Repository's **Secrets**.

1. Push this entire folder (excluding `.env`, `service_account.json`, and `__pycache__`) to a private GitHub repository.
2. On your GitHub repository page, go to **Settings** -> **Secrets and variables** -> **Actions**.
3. Click **New repository secret**.
4. You need to create a Secret for each of these items, pasting the value exactly as it appears in your `.env` file:
   - `GEMINI_API_KEY`
   - `EMAIL_SENDER_ADDRESS`
   - `EMAIL_SENDER_PASSWORD`
   - `GOOGLE_SHEET_ID`
   - `GOOGLE_SHEET_TAB_NAME`
5. **CRITICAL STEP**: Open your `service_account.json` file in a text editor (like Notepad), copy the **entire contents** of the file, and paste it into a new GitHub Secret named:
   - `GCP_SERVICE_ACCOUNT_JSON`

Once these secrets are saved, GitHub will automatically run the script every day at **01:00 UTC** (which is 08:00 AM ICT / Vietnam Time). You can also click the **Actions** tab in your repo, select "Daily Campaign Spend Alert", and click "Run workflow" to test it immediately!
