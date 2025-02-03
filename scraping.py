from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import requests

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
                          quantity INTEGER,
                          review_count INTEGER,
                          PRIMARY KEY (id)
                      )
                      """))
    
    conn.commit()
    print("Connection made successfully and table was created successfully!!")
    return conn

connect_to_db(user='postgres', password='oladipupo', database_name="Books_db")

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
        soup = scrape_book_data(url)
        # Extract the book details
        rows = soup.find("ol", class_='row').find_all("li")
        for row in rows:
            book_names.append(row.find("h3").find("a")["title"])
            book_prices.append(float(row.find('p', class_='price_color').text[2:]))
            
    return {
        "names": book_names,
        "prices": book_prices
    }
        
def insert_book_data(conn: object, book_details: dict) -> None:
    ...   
    
# Combine lists into a list of books (dictionaries)
def prepare_books_data(names, prices, quantities, review_counts):
    books = []
    for i in range(len(names)):
        book = {
            "name": names[i],
            "price": prices[i],
            "quantity": quantities[i],
            "review_count": review_counts[i]
        }
        books.append(book)
    return books

# Insert books into the database
def insert_books_into_db(books, conn):
    try:
        with conn.begin() as transaction:
            for book in books:
                conn.execute(text("""
                    INSERT INTO Books (name, price, quantity, review_count)
                    VALUES (:name, :price, :quantity, :review_count)
                """), book)
        print(f"Inserted {len(books)} books into the database successfully!")
    except Exception as e:
        print(f"Error while inserting books into the database: {e}")