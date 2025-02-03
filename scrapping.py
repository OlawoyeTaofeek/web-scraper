from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import requests
from typing import Dict, List

def connect_to_db(**kwargs):
    # Establish a connection to the database
    engine = create_engine(f"postgresql+psycopg://{kwargs['user']}:{kwargs['password']}@localhost:5433/{kwargs['database_name']}")
    ## create connection to database
    conn = engine.connect()
    # create a table in the database
    conn.execute(text("""
                      CREATE TABLE if not exists Books (
                          id SERIAL NOT NULL,
                          name TEXT NOT NULL,
                          price float NOT NULL,
                          quantity TEXT,
                          review_count INTEGER,
                          PRIMARY KEY (id)
                      )
                      """))
    
    conn.commit()
    print("Connection made successfully and table was created successfully!!")
    return conn

def scrape_book_data(url: str) -> BeautifulSoup:
    # Send a GET request to the webpage
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup

def extract_book_details() -> dict:
    book_names = []
    book_prices = []
    for i in range(1, 51):
        # URL of the book webpage
        url = f"https://books.toscrape.com/catalogue/page-{i}.html"
        stew = scrape_book_data(url)
        # Extract the book details
        rows = stew.find("ol", class_='row').find_all("li")
        for row in rows:
            book_names.append(row.find("h3").find("a")["title"])
            book_prices.append(float(row.find('p', class_='price_color').text[2:]))
            
    return {
        "names": book_names,
        "prices": book_prices
    }
    
def get_book_quantity_review() -> Dict[str, List]:
    # Extract book details
    book_details = extract_book_details()
    book_names = book_details["names"]
    book_prices = book_details["prices"]

    book_quantities = []  # For stock availability
    book_review_counts = []  # For review counts

    i, j = 1000, 0  # Start with 1000 and iterate through the book_names
    while i > 0 and j < len(book_names):
        # Generate the URL with the pattern
        url = f'https://books.toscrape.com/catalogue/{book_names[j].lower().replace(" ", "-")}_{i}/index.html'

        # Fetch and parse the book's individual page
        soup = scrape_book_data(url)

        # Extract stock availability (book quantities)
        stock_info = soup.find('p', class_='instock availability')
        if stock_info:
            book_quantities.append(stock_info.text.replace("\n", "").strip())
        else:
            book_quantities.append("Unknown")  # Fallback if stock info is missing

        # Extract review counts
        review_table = soup.find_all('tr')
        if review_table and review_table[-1].td:
            book_review_counts.append(int(review_table[-1].td.text))
        else:
            book_review_counts.append(0)  # Fallback if no review count is found

        # Decrement `i` and increment `j`
        i -= 1
        j += 1

    return {
        "names": book_names,
        "prices": book_prices,
        "quantities": book_quantities,
        "review_counts": book_review_counts
    }
       
def insert_book_data(conn: object, book_details: dict) -> None:
    for name , price, quantity, review in zip(book_details['names'], book_details['prices'], 
                                              book_details['quantities'], book_details['review_counts']):
        # Insert book data into the database
        conn.execute(text("""
                          INSERT INTO Books (name, price, quantity, review_count)
                          VALUES (:name, :price, :quantity, :review)"""), 
                     {'name': name, 'price': price, 'quantity': quantity, 'review_count': review})
        
    conn.commit()
    print("Book data was inserted successfully!")
    
    
books_detail = get_book_quantity_review()

# Create a connection to the database

conn = connect_to_db(user='postgres', password='oladipupo', database_name="Books_db")

# Connect to the database
insert_book_data(conn, books_detail) 