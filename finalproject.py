import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'country.db'
GDPCSV = '2014_world_gdp_with_codes.csv'
COUNTRIESJSON = 'countries.json'


def init_db():
    print('Creating Database.')
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except Error as e:
        print(e)

    # Drop table if already exists
    statement = '''
        DROP TABLE IF EXISTS 'GDP';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'Countries';
    '''
    cur.execute(statement)

    statement = '''
        CREATE TABLE 'GDP' (
            'ID' INTEGER PRIMARY KEY AUTOINCREMENT,
            'COUNTRY' TEXT NOT NULL,
            'GDP (Billions)' INTEGER NOT NULL,
            'Code' TEXT NOT NULL
        );
    '''
    cur.execute(statement)

    generate = '''
        CREATE TABLE 'Countries' (
            'ID' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Alpha2' TEXT NOT NULL,
            'Alpha3' TEXT NOT NULL,
            'EnglishName' TEXT NOT NULL,
            'Region' TEXT NOT NULL,
            'Subregion' TEXT NOT NULL,
            'Population' INTEGER NOT NULL,
            'Area' REAL
        );
    '''
    cur.execute(generate)
    conn.commit()

    conn.close()


def insert_data():
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except Error as e:
        print(e)

    print('Inserting Data.')

    fref = open(COUNTRIESJSON, 'r')
    data = fref.read()
    results_list = json.loads(data)

# Column order in COUNTRIESJSON file: ID ,Alpha2,Alpha3,EnglishName,Region,
# Subregion, population, area

    for results in results_list:
        insertion = (None, results['alpha2Code'], results['alpha3Code'], results['name'],
                     results['region'], results['subregion'], results['population'],
                     results['area'])
        statement = 'INSERT INTO "Countries" '
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement, insertion)

    with open(GDPCSV) as csvDataFile:
        csvReader = csv.reader(csvDataFile)
        next(csvReader, None)
    # Column order in GDP.csv file: Country, GDP, Code
        for row in csvReader:
            insertion = (None, row[0], row[1], row[2])
            statement = 'INSERT INTO "GDP" '
            statement += 'VALUES (?, ?, ?, ?)'
            cur.execute(statement, insertion)

        conn.commit()
        conn.close()

init_db()
insert_data()
