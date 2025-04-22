# Dutch Real Estate Scraper

A comprehensive system for scraping, monitoring, and receiving notifications about real estate listings in the Netherlands.

## Features

- Scrapes multiple Dutch real estate websites (Funda, Pararius)
- Telegram bot for user interaction and property notifications
- Property filtering based on user preferences
- Containerized with Docker for easy deployment
- Compatible with Portainer for simplified management

## System Components

1. **Real Estate Scraper**: Retrieves property listings from various websites
2. **Telegram Bot**: Provides a user interface and sends notifications
3. **PostgreSQL Database**: Stores property data, user preferences, and notification status

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Git
- A Telegram bot token (from [@BotFather](https://t.me/botfather))

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/KevinHang/dutchestatescraper.git
   cd dutch-realestate-scraper
   ```

2. Run the setup script:
   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```

3. Edit the `.env` file with your configuration:
   ```bash
   nano .env
   ```
   
   Make sure to set:
   - `TELEGRAM_BOT_TOKEN`: Your Telegram bot token
   - `TELEGRAM_ADMIN_USER_IDS`: Your Telegram user ID
   - Other configuration options as needed

4. Start the services:
   ```bash
   docker-compose up -d
   ```

### Hetzner Cloud Deployment with Portainer

For detailed instructions on deploying with Portainer on Hetzner Cloud, see [DEPLOYMENT.md](DEPLOYMENT.md).

## Usage

### Managing Query URLs

Add specific search URLs to monitor:

```bash
docker-compose exec scraper python -m cli --add-query-url "funda:https://www.funda.nl/huur/amsterdam/beschikbaar/" --query-method "GET" --add-query-description "Amsterdam rentals on Funda"
```

List configured URLs:

```bash
docker-compose exec scraper python -m cli --list-query-urls
```

### Telegram Bot Commands

- `/start` - Register with the bot
- `/help` - Display available commands
- `/search` - Search for properties
- `/settings` - Configure your notification preferences
- `/stats` - View system statistics (admin only)

## Development

### Project Structure

```
dutch-realestate-scraper/
├── config.py                  # Configuration settings
├── main.py                    # Scraper entry point
├── main_telegram.py           # Telegram bot entry point
├── cli.py                     # Command-line interface
├── database/                  # Database operations
├── models/                    # Data models
├── scrapers/                  # Website-specific scrapers
├── telegram_bot/              # Telegram bot implementation
├── utils/                     # Utility functions
├── docker-compose.yml         # Docker Compose configuration
├── Dockerfile                 # Docker build instructions
└── requirements.txt           # Python dependencies
```

### Adding a New Scraper

1. Create a new scraper class in `scrapers/` directory
2. Add it to the scraper factory in `scrapers/factory.py`
3. Add the source name to `DEFAULT_SOURCES` in your configuration

## Maintenance

### Backup

Backup the database:

```bash
docker-compose exec postgres pg_dump -U postgres realestate > backup_$(date +%Y%m%d).sql
```

### Updating

Update to the latest version:

```bash
git pull
docker-compose up -d --build
```

## Troubleshooting

See [TROUBLESHOOTING.md](TROUBLESHOOTING.md) for common issues and solutions.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [python-telegram-bot](https://github.com/python-telegram-bot/python-telegram-bot) for the Telegram integration
- [PostGIS](https://postgis.net/) for geographic database capabilities
- [pgvector](https://github.com/pgvector/pgvector) for vector similarity search