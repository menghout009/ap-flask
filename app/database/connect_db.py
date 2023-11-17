import psycopg2


# Connect to the PostgreSQL database
conn = psycopg2.connect(
    dbname='data_vue',
    user='data_vue',
    password='data_vue',
    host='110.74.194.123',
    port='5436'
)
