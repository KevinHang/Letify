# Telegram Bot Setup Guide

This guide will walk you through setting up the Telegram bot integration for the Dutch Real Estate Scraper.

## Prerequisites

- Python 3.7+
- PostgreSQL database
- A Telegram account

## Step 1: Create a Telegram Bot

1. Open Telegram and search for "BotFather" (@BotFather)
2. Start a chat with BotFather and send the command `/newbot`
3. Follow the instructions to name your bot:
   - First provide a display name (e.g., "Dutch Real Estate Bot")
   - Then provide a username (e.g., "dutch_real_estate_bot") - must end with "bot"
4. BotFather will respond with a message containing your bot token. This looks like:
   ```
   123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi
   ```
5. Save this token securely - it will be used in your configuration

## Step 2: Configure Bot Settings (optional)

While still in chat with BotFather, you can configure additional settings:

1. Set description: `/setdescription`
2. Set about text: `/setabouttext`
3. Set profile picture: `/setuserpic`
4. Set commands: `/setcommands`

For commands, you can use:
```
start - Start the bot and see welcome message
help - Show available commands
preferences - Set your property search preferences
subscribe - Start receiving notifications
unsubscribe - Stop receiving notifications
status - Check your current settings
```

## Step 3: Install Dependencies

Add these lines to your `requirements.txt` file:

```
python-telegram-bot>=20.0
```

Then install the dependencies:

```bash
pip install -r requirements.txt
```

## Step 4: Update Configuration

Edit your `config.py` file to include the Telegram settings:

```python
# Telegram bot settings
ENABLE_TELEGRAM = True
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"  # The token from BotFather
TELEGRAM_ADMIN_USER_IDS = []  # Leave empty for now, will be populated later
NOTIFICATION_INTERVAL = 300  # 5 minutes in seconds
MAX_NOTIFICATIONS_PER_USER_PER_DAY = 20
NOTIFICATION_BATCH_SIZE = 50
NOTIFICATION_RETRY_ATTEMPTS = 3
```

## Step 5: Set Up Database Tables

The Telegram integration requires additional database tables. Use the provided migration script to create them:

```bash
# Run the main script with the --init-db flag
python main_telegram.py --init-db
```

This will create the necessary tables for Telegram users, preferences, notifications, etc.

## Step 6: Starting Your Bot

Start the bot with the main Telegram script:

```bash
python main_telegram.py
```

This will start both the real estate scraper and the Telegram bot.

## Step 7: Becoming an Admin

To make yourself an admin:

1. Start a chat with your bot by finding it in Telegram (using the username you created)
2. Send the `/start` command to register yourself
3. Find your Telegram user ID:
   - Either use a bot like @userinfobot
   - Or look in your database - your user will be in the `telegram_users` table
4. Update your `config.py` file with your user ID:
   ```python
   TELEGRAM_ADMIN_USER_IDS = [YOUR_USER_ID_HERE]  # e.g., [123456789]
   ```
5. Restart the bot
6. Now you should have access to admin commands

## Step 8: Bot Commands and Usage

### For Regular Users:

- `/start` - Begin interaction with the bot
- `/help` - See available commands
- `/preferences` - Set property search preferences
- `/subscribe` - Start receiving notifications
- `/unsubscribe` - Stop receiving notifications
- `/status` - Check current settings

### For Admin Users:

- `/admin` - Access admin menu
- `/broadcast` - Send message to all users
- `/stats` - View bot statistics

## Step 9: Managing Your Bot

### Hosting Options

For long-term hosting, consider:

1. **VPS/Dedicated Server**: Providers like DigitalOcean, Linode, AWS, etc.
2. **PythonAnywhere**: Good for Python web applications with scheduled tasks
3. **Heroku**: Platform as a Service with free tier available

### Server Setup Tips

1. Use a process manager like `systemd`, `supervisor`, or `pm2` to keep your bot running
2. Set up log rotation to manage log files
3. Consider using Docker for easier deployment

### Example Systemd Service

Create a file `/etc/systemd/system/real-estate-bot.service`:

```ini
[Unit]
Description=Dutch Real Estate Telegram Bot
After=network.target postgresql.service

[Service]
User=your_username
WorkingDirectory=/path/to/dutch-real-estate-scraper
ExecStart=/path/to/python /path/to/dutch-real-estate-scraper/main_telegram.py
Restart=always
RestartSec=10
StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=real-estate-bot

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```bash
sudo systemctl enable real-estate-bot
sudo systemctl start real-estate-bot
```

## Step 10: Monitoring and Maintenance

- Check the bot's logs regularly
- Set up error notifications to admin users
- Periodically clean up old data
- Update your bot token if it gets compromised

## Troubleshooting

### Bot Not Responding
- Check if the script is running
- Verify the bot token is correct
- Ensure your database is accessible

### Database Issues
- Check PostgreSQL connection
- Verify that all migrations have run successfully

### Telegram API Limits
- Telegram has rate limits
- Be careful with broadcast messages to many users
- Use the `notification_interval` setting to pace your notifications

## Security Considerations

- Keep your bot token secure
- Don't store sensitive user data
- Be cautious with admin privileges
- Validate user input to prevent SQL injection