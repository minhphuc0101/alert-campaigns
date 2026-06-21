# Telegram Ad Bot

An AI-powered Telegram bot for managing and analyzing Meta Ads and Google Ads campaigns via natural language.

## Features
- Turn campaigns on/off using natural language ("Turn off the summer campaign").
- Update budgets ("Increase budget for campaign X to 500k").
- Fetch analytical data ("How much did we spend yesterday on account Y?").
- Automated rule management (Thanos Rules) to auto-pause campaigns based on spend and KPIs.

## Setup
1. Clone the repository.
2. Install dependencies: `pip install -r requirements.txt`
3. Set up your `.env` file with API keys (Telegram, Gemini, Meta, Google Ads).
4. Run the bot: `python bot.py`
