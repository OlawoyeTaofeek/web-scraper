# Books Scraper

## Overview
This project is a web scraper that extracts book data from [Books to Scrape](https://books.toscrape.com/). It scrapes book details such as names, prices, stock availability, and review counts from multiple pages and stores them in a PostgreSQL database.

## Features
- **Asynchronous Web Scraping**: Uses `aiohttp` and `asyncio` to efficiently fetch data.
- **BeautifulSoup Parsing**: Extracts book details from HTML pages.
- **PostgreSQL Database Storage**: Saves the scraped data into a structured database.
- **Error Handling & Optimization**: Ensures smooth execution while scraping up to 50 pages.

## Technologies Used
- Python
- `asyncio`, `aiohttp`
- `BeautifulSoup` for parsing HTML
- `SQLAlchemy` for database interaction
- PostgreSQL

## Installation
```sh
# Clone the repository
git clone https://github.com/yourusername/books-scraper.git
cd books-scraper

# Create a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows, use: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
