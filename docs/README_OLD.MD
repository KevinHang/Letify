# Dutch Real Estate Scraper - CLI Reference

## Overview

The Dutch Real Estate Scraper is a configurable tool for scraping real estate listings from popular Dutch property websites (e.g., Funda, Pararius) and notifying users via a Telegram bot. The system is split into two main components:
- **Scraper** (`main.py`, invoked via `cli.py`): Scrapes listings and queues notifications in a database.
- **Telegram Bot** (`main_telegram.py`): Runs the bot for user interactions and sends notifications from the queue.

This document provides a complete reference for the command-line interface (`cli.py`) and instructions for running the Telegram bot.

## Installation

Ensure you have Python 3.8+ installed, then:

```bash
# Clone the repository (replace with your actual repository URL)
git clone <your-repository-url>
cd dutch-real-estate-scraper

# Install dependencies
pip install -r requirements.txt
Create a .env file or set environment variables for configuration (see Environmental Configuration (#environmental-configuration)).
Basic Usage
Scraper (cli.py)
The scraper is controlled via cli.py, which invokes main.py to scrape listings and queue notifications.
Scan Modes
The scraper supports three scanning modes:
bash
# City-based scanning (extracts listings from city search pages)
python cli.py --city-scan --sources funda,pararius --cities amsterdam,rotterdam

# Query URL scanning (uses URLs stored in the database)
python cli.py --query-scan --sources funda,pararius

# Combined scanning (both cities and query URLs)
python cli.py --combined-scan --sources funda,pararius --cities amsterdam,rotterdam
Default behavior (if no mode specified):
bash
# Defaults to city-based scanning when cities are provided
python cli.py --sources funda,pararius --cities amsterdam,rotterdam
Running Options
bash
# Run once and exit
python cli.py --city-scan --sources funda,pararius --cities amsterdam,rotterdam --once

# Run continuously with a specific interval (in seconds)
python cli.py --city-scan --sources funda,pararius --cities amsterdam,rotterdam --interval 3600
Telegram Bot (main_telegram.py)
The Telegram bot runs independently to handle user interactions and send notifications:
bash
# Start the Telegram bot and notification manager
python main_telegram.py
The bot processes notifications queued by the scraper and responds to user commands (e.g., /preferences, /subscribe).
Query URL Management
Query URLs allow scanning specific search results pages, offering more flexibility than city-based scanning.
Adding Query URLs
bash
# Add a query URL (enabled by default)
python cli.py --add-query-url "funda:https://www.funda.nl/zoeken/huur?object_type=[\"apartment\"]&sort=\"date_down\"" --add-query-description "Amsterdam apartments"

# Add with POST method
python cli.py --add-query-url "funda:https://www.funda.nl/zoeken/huur?object_type=[\"apartment\"]" --query-method POST

# Add in disabled state
python cli.py --add-query-url "funda:https://www.funda.nl/zoeken/huur?object_type=[\"apartment\"]" --disable
Special URL parameters should be escaped with backslashes before double quotes.
Managing Query URLs
bash
# List all query URLs
python cli.py --list-query-urls

# Toggle enabled status of a query URL
python cli.py --toggle-query-url 1

# Delete a query URL
python cli.py --delete-query-url 1
Telegram Integration
The Telegram bot notifies users about new property listings matching their preferences. The bot runs via main_telegram.py, while cli.py provides user management commands.
Setup and Configuration
Create a bot through BotFather on Telegram and obtain the bot token.
Add the token to your .env file or environment variables (see Environmental Configuration (#environmental-configuration)).
Initialize the Telegram database tables:
bash
python cli.py --init-telegram-db
User Management
bash
# List all registered Telegram users
python cli.py --list-telegram-users

# Make a user an admin (replace 123456789 with the Telegram user ID)
python cli.py --make-admin 123456789

# Revoke admin status
python cli.py --revoke-admin 123456789
Broadcasts
bash
# Send a broadcast message to admin users (and optionally all users)
python cli.py --send-broadcast "Important message: Maintenance scheduled tonight" --telegram-token "your_bot_token"
Advanced Telegram Options
bash
# Override the bot token
python cli.py --telegram-token "your_bot_token_here" --send-broadcast "Test message"

# Set custom admin user IDs (comma-separated)
python cli.py --telegram-admin "123456789,987654321" --send-broadcast "Test message"
Additional Options
Sources and Limits
bash
# List available sources
python cli.py --list-sources

# Specify custom result limits
python cli.py --max-results 200 --max-concurrent 10
Proxy Configuration
bash
# Enable proxy usage
python cli.py --use-proxies

# Disable proxy usage
python cli.py --no-proxies

# Use specific proxies
python cli.py --proxy-list "http://proxy1.example.com,http://proxy2.example.com"

# Set proxy rotation strategy
python cli.py --proxy-rotation round_robin  # Options: round_robin, random, fallback

# Display proxy statistics
python cli.py --proxy-stats
Debug Mode
bash
# Enable debug logging
python cli.py --debug
Environmental Configuration
Settings can be configured via environment variables or a .env file:
# Database settings
DB_HOST=localhost
DB_PORT=5432
DB_NAME=realestate
DB_USER=postgres
DB_PASSWORD=postgres

# Scraper settings
DEFAULT_SCAN_INTERVAL=3600
DEFAULT_CITIES=amsterdam,rotterdam,utrecht,den-haag,eindhoven
DEFAULT_SOURCES=funda,pararius
MAX_RESULTS_PER_SCAN=100
MAX_CONCURRENT_REQUESTS=5

# Proxy settings
USE_PROXIES=False
PROXY_LIST=http://proxy1.example.com,http://proxy2.example.com
PROXY_ROTATION_STRATEGY=round_robin

# Telegram settings
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_ADMIN_USER_IDS=123456789,987654321
NOTIFICATION_INTERVAL=300
MAX_NOTIFICATIONS_PER_USER_PER_DAY=20
NOTIFICATION_BATCH_SIZE=50
NOTIFICATION_RETRY_ATTEMPTS=3
Command Reference
Option
Description
--city-scan
City-based scanning mode
--query-scan
Query URL scanning mode
--combined-scan
Combined scanning mode
--sources SOURCES
Comma-separated list of sources to scrape
--cities CITIES
Comma-separated list of cities to scrape
--interval INTERVAL
Scraping interval in seconds
--once
Run only one scan cycle
--add-query-url URL
Add a query URL (format: source:url)
--query-method {GET,POST}
HTTP method for the query URL
--disable
Add the query URL in disabled state
--add-query-description DESC
Description for the query URL
--list-query-urls
List all query URLs in the database
--toggle-query-url ID
Toggle the enabled status of a query URL
--delete-query-url ID
Delete a query URL
--use-proxies
Enable proxy usage
--no-proxies
Disable proxy usage
--proxy-list PROXIES
Comma-separated list of proxy URLs
--proxy-rotation STRATEGY
Proxy rotation strategy
--proxy-stats
Display proxy statistics after scanning
--max-results LIMIT
Maximum results to process per scan
--max-concurrent LIMIT
Maximum concurrent requests
--debug
Enable debug logging
--list-sources
List available sources
--telegram-token TOKEN
Override the Telegram bot token
--telegram-admin IDS
Comma-separated admin user IDs
--init-telegram-db
Initialize Telegram database tables
--list-telegram-users
List registered Telegram users
--make-admin USER_ID
Make a user an admin
--revoke-admin USER_ID
Revoke admin status
--send-broadcast MSG
Send message to admin users (optionally all users)
Using the Telegram Bot
To start the Telegram bot:
bash
python main_telegram.py
Once running, users can interact with the bot:
Find the bot on Telegram by its username and start a chat.
Available commands:
/start - Begin interaction with the bot.
/help - See available commands.
/preferences - Set property search preferences (e.g., city, price range). The city selection menu has been fixed for reliable input.
/subscribe - Start receiving notifications.
/unsubscribe - Stop receiving notifications.
/status - Check current settings.
Users receive notifications about new listings matching their preferences, with options to like, dislike, save, or view details.
Logs are clean, with no /getUpdates spam.
Admin users have additional commands:
/admin - Access admin menu.
/broadcast - Send message to all users.
/stats - View bot statistics.
Running Both Components
To run the full system:
Start the Telegram bot in one terminal:
bash
python main_telegram.py
Run the scraper in another terminal:
bash
python cli.py --sources funda,pararius --cities amsterdam,rotterdam
The scraper queues notifications, and the bot sends them to users. Logs are written to scraper.log (scraper), telegram.log (bot), and realestate_scraper.log (CLI).
Troubleshooting
No notifications received: Ensure main_telegram.py is running, check notification_queue table, and verify TELEGRAM_BOT_TOKEN.
City button not working: Confirm the latest telegram_bot.py is used, and check telegram.log for errors.
Database errors: Verify DB_CONNECTION_STRING and run migrations (initialize_db, initialize_telegram_db).
Log spam: /getUpdates logs are suppressed; if other logs are verbose, use --debug sparingly.
For issues, check logs (scraper.log, telegram.log, realestate_scraper.log) and share errors with the repository maintainers.

---

### Explanation of Changes

1. **Separated Scraper and Telegram Bot**:
   - Added an **Overview** section to explain the split into `main.py` (scraper) and `main_telegram.py` (Telegram bot).
   - Introduced a **Telegram Bot (`main_telegram.py`)** section with instructions to run `python main_telegram.py`.
   - Updated **Running Both Components** to describe running both processes simultaneously.

2. **Updated CLI Commands**:
   - Removed `--enable-telegram` and `--disable-telegram` from the command reference, as the Telegram bot is managed separately.
   - Kept `--telegram-token` and `--telegram-admin` for broadcast commands, aligning with `cli.py`.
   - Retained query URL, proxy, and user management commands unchanged.

3. **Telegram Bot Usage**:
   - Clarified that the bot is started with `main_telegram.py`, not `cli.py`.
   - Noted the fixed “City” button in `/preferences` for clarity.
   - Mentioned log suppression for `/getUpdates`.

4. **Environmental Configuration**:
   - Kept the `.env` example unchanged, as it’s still valid.
   - Removed `ENABLE_TELEGRAM` from the example, as it’s no longer used.

5. **Troubleshooting**:
   - Added specific guidance for common issues (e.g., notifications not sent, city button issues).
   - Referenced all three log files (`scraper.log`, `telegram.log`, `realestate_scraper.log`).

6. **Repository Reference**:
   - Replaced the placeholder `git clone` URL with a generic `<your-repository-url>` to avoid confusion.
   - Noted that users should use their actual repository or local setup.

7. **Command Invocation**:
   - Changed `python -m cli` to `python cli.py` for consistency with direct script execution, as the project is not a Python module.

---

### Integration with Project

To use the updated `README.md`:
1. Save it as `README.md` in the project root:
   project/
   ├── README.md
   ├── cli.py
   ├── main.py
   ├── main_telegram.py
   ├── telegram_bot/
   │   ├── telegram_bot.py
   │   ├── telegram_notification_manager.py
   ├── database/
   │   ├── property_db.py
   │   ├── telegram_db.py
   │   ├── migrations.py
   ├── scrapers/
   │   ├── factory.py
   ├── utils/
   │   ├── http.py
   │   ├── proxy_manager.py
   │   ├── formatting.py
   ├── config.py
   ├── requirements.txt
   ├── realestate_scraper.log
   ├── scraper.log
   ├── telegram.log
2. Update `requirements.txt` to include all dependencies:
   python-telegram-bot>=20.0
   psycopg2-binary>=2.9
   aiohttp>=3.8
3. Ensure `config.py` matches the `.env` example in the README.

### Testing the Documentation

1. **Verify CLI Commands**:
- Run each command listed in the **Command Reference** (e.g., `python cli.py --list-sources`, `python cli.py --send-broadcast "Test"`).
- Check that outputs and logs match the descriptions.

2. **Test Telegram Bot Instructions**:
- Start the bot: `python main_telegram.py`.
- Use `/preferences`, confirm the “City” button works, and check `telegram.log` for clean output.
- Verify notifications are sent when running `python cli.py --sources funda --cities amsterdam --once`.

3. **Check README Clarity**:
- Follow the **Installation** and **Basic Usage** sections as a new user.
- Ensure all commands work as described and logs are clean (no `/getUpdates` spam).

4. **Troubleshooting Section**:
- Simulate a failure (e.g., invalid `TELEGRAM_BOT_TOKEN`) and follow the troubleshooting steps.
- Confirm logs provide enough detail to diagnose issues.

---

### Potential Issues

- **Outdated Repository URL**: If using a specific repository, replace `<your-repository-url>` with the actual URL.
- **Missing Dependencies**: Ensure `requirements.txt` includes all packages, or users may encounter import errors.
- **Command Syntax**: Users accustomed to `python -m cli` may need to adjust to `python cli.py`; clarify in your documentation if this is a concern.
- **Environment Variables**: If `.env` is not set up, users may miss required settings; consider adding a sample `.env` file in the repository.

---

### Next Steps

Please place the updated `README.md` in your project and test the instructions. Confirm:
- All CLI commands work as described.
- The Telegram bot starts correctly with `main_telegram.py`, and the “City” button functions.
- Logs are clean and troubleshooting steps are effective.
- Any additional sections or clarifications needed in the README.

If you need further updates (e.g., other documentation files, sample `.env`, or additional CLI features), let me know!