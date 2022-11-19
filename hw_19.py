import requests
import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from hw_19_models import create_tables, drop_tables, Publisher, Book, Shop, Stock, Sale

class ContentMaker:
    
    def __init__(self):
        self.url = 'https://raw.githubusercontent.com/netology-code/py-homeworks-db/video/06-orm/fixtures/tests_data.json'
        
    def get_query(self):
        """Method gets response from server and returns dictionary"""
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            return json.loads(response.text)

        except(requests.RequestException, ValueError):
            print('Ошибка')
            return False

class DatabaseHandler:

    def __init__(self):
        self.DSN = 'postgresql://postgres:postgres@localhost:5432/hw_19'
        self.engine = sqlalchemy.create_engine(self.DSN)
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        create_tables(self.engine)

    def drop_tables(self):
        drop_tables(self.engine)

    def db_complition(self):
        content = ContentMaker()

        session = self.Session()

        if content.get_query() != False:
            for data in content.get_query():
                if data['model'] == 'publisher':
                    data_to_add = Publisher(name=data['fields']['name'])
                    session.add(data_to_add)
            
                if data['model'] == 'book':
                    data_to_add = Book(title=data['fields']['title'], id_publisher=data['fields']['id_publisher'])
                    session.add(data_to_add)

                if data['model'] == 'shop':
                    data_to_add = Shop(name=data['fields']['name'])
                    session.add(data_to_add)
                
                if data['model'] == 'stock':
                    data_to_add = Stock(id_shop=data['fields']['id_shop'], id_book=data['fields']['id_book'], count=data['fields']['count'])
                    session.add(data_to_add)

                if data['model'] == 'sale':
                    data_to_add = Sale(price=data['fields']['price'], date_sale=data['fields']['date_sale'], count=data['fields']['count'], id_stock=data['fields']['id_stock'])
                    session.add(data_to_add)

        session.commit()
        session.close()

    def search_by_name(self, comm):
    
        session = self.Session()

        q = (session.query(Book)
                    .join(Book.publisher)
                    .join(Book.stock)
                    .join(Stock.shop)
                    .join(Stock.sale)
                    .filter(Publisher.name.like(comm)))

        for book in q.all():
            books_title = book.title
            for stock in book.stock:
                shops_name = stock.shop.name
                for sale in stock.sale:
                    price = sale.price
                    date = sale.date_sale
                    print('{:40s} | {:15s} | {:.2f} | {:.10s}'.format(books_title, shops_name, price, str(date)))
                    
        session.close()

    def search_by_id(self, comm):
        
        session = self.Session()

        q = (session.query(Book)
                    .join(Book.publisher)
                    .join(Book.stock)
                    .join(Stock.shop)
                    .join(Stock.sale)
                    .filter(Book.id_publisher == comm))

        for book in q.all():
            books_title = book.title
            for stock in book.stock:
                shops_name = stock.shop.name
                for sale in stock.sale:
                    price = sale.price
                    date = sale.date_sale
                    print('{:40s} | {:15s} | {:.2f} | {:.10s}'.format(books_title, shops_name, price, str(date)))

        session.close()

if __name__ == '__main__':


    handler = DatabaseHandler()

    handler.drop_tables()

    handler.create_tables()

    handler.db_complition()

    while True:

        comm = input('Введите имя или идентификатор издателя: ')

        if comm.isdigit():
            handler.search_by_id(comm)
        elif comm == 'q':
            break
        elif comm != '':
            handler.search_by_name(comm)
        else:
            print('Для выхода введите q\n')
        
