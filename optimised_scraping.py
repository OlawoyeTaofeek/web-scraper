import asyncio
import aiohttp
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from typing import Dict, List


def connect_to_db(**kwargs):
    engine = create_engine(f"postgresql+psycopg://{kwargs['user']}:{kwargs['password']}@localhost:5433/{kwargs['database_name']}")
    conn = engine.connect()
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS Books (
            id SERIAL NOT NULL,
            name TEXT NOT NULL,
            price FLOAT NOT NULL,
            quantity TEXT,
            review_count INTEGER,
            PRIMARY KEY (id)
        )
    """))
    conn.commit()
    print("Connection established and table created successfully!")
    return conn


async def fetch_html(session, url):
    async with session.get(url) as response:
        return await response.text()


async def scrape_book_list_pages():
    base_url = "https://books.toscrape.com/catalogue/page-{}.html"
    tasks = []

    async with aiohttp.ClientSession() as session:
        for i in range(1, 51):  # Pages 1 to 50
            url = base_url.format(i)
            tasks.append(fetch_html(session, url))

        pages = await asyncio.gather(*tasks)
    return pages


def parse_list_pages(pages: List[str]) -> Dict[str, List]:
    book_names = []
    book_prices = []

    for page in pages:
        soup = BeautifulSoup(page, "html.parser")
        rows = soup.find("ol", class_="row").find_all("li")
        for row in rows:
            book_names.append(row.find("h3").find("a")["title"])
            book_prices.append(float(row.find("p", class_="price_color").text[2:]))

    return {"names": book_names, "prices": book_prices}


async def scrape_individual_book_pages(book_names):
    base_url = "https://books.toscrape.com/catalogue/{}_{}.html"
    tasks = []

    async with aiohttp.ClientSession() as session:
        for i, book_name in enumerate(book_names):
            slug = book_name.lower().replace(" ", "-").replace("'", "").replace(",", "").replace(":", "")
            url = base_url.format(slug, 1000 - i)
            tasks.append(fetch_html(session, url))

        pages = await asyncio.gather(*tasks)
    return pages


def parse_individual_pages(pages: List[str]) -> Dict[str, List]:
    book_quantities = []
    book_review_counts = []

    for page in pages:
        soup = BeautifulSoup(page, "html.parser")
        # Extract stock availability
        stock_info = soup.find("p", class_="instock availability")
        book_quantities.append(stock_info.text.replace("\n", "").strip() if stock_info else "Unknown")

        # Extract review counts
        review_table = soup.find_all("tr")
        if review_table and review_table[-1].td:
            book_review_counts.append(int(review_table[-1].td.text))
        else:
            book_review_counts.append(0)

    return {"quantities": book_quantities, "review_counts": book_review_counts}


async def get_book_data():
    # Scrape list pages
    list_pages = await scrape_book_list_pages()
    book_details = parse_list_pages(list_pages)

    # Scrape individual book pages
    individual_pages = await scrape_individual_book_pages(book_details["names"])
    individual_details = parse_individual_pages(individual_pages)

    # Combine data
    book_details.update(individual_details)
    return book_details


def save_to_db(conn, book_data):
    for name, price, quantity, review_count in zip(
        book_data["names"], book_data["prices"], book_data["quantities"], book_data["review_counts"]
    ):
        conn.execute(
            text("""
                INSERT INTO Books (name, price, quantity, review_count)
                VALUES (:name, :price, :quantity, :review_count)
            """),
            {"name": name, "price": price, "quantity": quantity, "review_count": review_count}
        )
    conn.commit()
    print("Data saved to database!")



# Main Execution
async def main():
    conn = connect_to_db(user="postgres", password="oladipupo", database_name="Books_db")
    book_data = await get_book_data()
    save_to_db(conn, book_data)


# Run the program
asyncio.run(main())
