![Dutch Real Estate Scraper](https://github.com/KevinHang/Letify/blob/main/media/letify_banner.png)  

![GitHub Repo stars](https://img.shields.io/github/stars/KevinHang/Letify?style=social) ![GitHub forks](https://img.shields.io/github/forks/KevinHang/Letify?style=social) ![GitHub watchers](https://img.shields.io/github/watchers/KevinHang/Letify?style=social)

*Monitor rental listings from Funda, Pararius, and more — with Telegram alerts.*

__*Having gained over 1000 users in the first months*__

__*Open-sourced on November 1st, 2025*__

---

> *The project has not been taken care of for several months. If the project gets more traction in the form of stars, then I will consider updating and improving this project. This project requires regular maintenance, as it might break for specific websites if the structure changes.*

---

## Letify Website

[https://www.letify.nl/](https://www.letify.nl/)

---

[![Star on GitHub](https://img.shields.io/github/stars/KevinHang/Letify?label=Star&style=for-the-badge&logo=github)](https://github.com/KevinHang/Letify) [![Fork on GitHub](https://img.shields.io/github/forks/KevinHang/Letify?label=Fork&style=for-the-badge&logo=github)](https://github.com/KevinHang/Letify/fork) [![Watch on GitHub](https://img.shields.io/github/watchers/KevinHang/Letify?label=Watch&style=for-the-badge&logo=github)](https://github.com/KevinHang/Letify/subscription)

---

## Overview

**Dutch Real Estate Scraper** is a modular, configurable tool to monitor rental listings from Dutch property platforms like **Funda** and **Pararius**. It scrapes new listings, stores them in a PostgreSQL database, and sends real-time notifications via a **Telegram bot**. It includes features such as agent rotation, and retry algorithms to prevent bot detection.

Perfect for house hunters, developers, or data enthusiasts who want to stay ahead in the Dutch rental market.

---

## Features

- Scrape city-based or custom query URLs  
- Run once or continuously with configurable intervals  
- **Agent rotation and retry strategies, to prevent bot detection**
- Proxy support (not fully tested)
- **Full Telegram bot integration: preferences, subscriptions, admin controls** 
- Clean logging, debug mode, and CLI-driven management  
- Extensible scraper architecture (add new sites easily)  

---

## Quick Start

```bash
git clone https://github.com/KevinHang/Letify.git
cd dutch-realestate-scraper

python -m venv venv
source venv/bin/activate  # venv\Scripts\activate on Windows
pip install -r requirements.txt
```

Create `.env` from the template and configure your database and Telegram token:

```bash
cp .env.template .env
# Edit .env with your DB and TELEGRAM_BOT_TOKEN
```

Initialize databases:

```bash
python cli.py --init-telegram-db
```

Start the **Telegram bot**:

```bash
python main_telegram.py
```

Run the **scraper** (in another terminal):

```bash
python cli.py --sources funda,pararius --cities amsterdam,rotterdam --interval 3600
```

---

## Documentation

**Note**: The files in the `docs/` folder (e.g., `getting-started-guide.md`, `installation-guide.md`) are might be **outdated**, please take take them for a grain of salt. I also included the old README `README_OLD.md` in the `docs/` folder, as it might include CLI useful information.

**Consult `README.md` and `cli.py --help` for the latest info.**

---

## Requirements

- Python 3.8+
- PostgreSQL 12+
- `python-telegram-bot>=20.0`, `psycopg2-binary`, `aiohttp`

---

## Disclaimer

This tool is for personal, non-commercial use. Respect website terms of service and rate limits. Use proxies responsibly.

---

**Made in Amsterdam**  
*No cookies, no tracking, just listings.*
