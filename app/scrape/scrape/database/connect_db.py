import psycopg2


# Connect to the PostgreSQL database
def connect_database():
    conn = psycopg2.connect(
        dbname='data_vue',
        user='data_vue',
        password='data_vue',
        host='110.74.194.123',
        port='5436'
    )
    return conn


# def connect_database():
#     conn = psycopg2.connect(
#         dbname='data_vue',
#         user='postgres',
#         password='3976',
#         host='localhost',
#         port='5433'
#     )
#     return conn
