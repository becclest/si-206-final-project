import sqlite3
import requests
import csv
import json
import time
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
import plotly.plotly as py
import plotly.graph_objs as go


DBNAME = 'country.db'
GDPCSV = '2014_world_gdp_with_codes.csv'

COUNTRIESJSON = 'countries.json'
CACHE_FNAME = 'cache.json'

politics_url = 'https://www.reddit.com/r/politics/.json'
world_politics_url = 'https://www.reddit.com/r/worldpolitics/.json'

# on startup, try to load the cache from file
CACHE_FNAME = 'cache.json'
try:
    cache_file = open(CACHE_FNAME, 'r')
    cache_contents = cache_file.read()
    CACHE_DICTION = json.loads(cache_contents)
    cache_file.close()

# if there was no file, no worries. There will be soon!
except:
    CACHE_DICTION = {}


def get_unique_key(url):
    return url

# The main cache function: it will always return the result for this
# url+params combo. However, it will first look to see if we have already
# cached the result and, if so, return the result from cache.
# If we haven't cached the result, it will get a new one (and cache it)


def make_domestic_request_using_cache(politics_url):
    header = {'User-Agent': 'windows:r/politics.single.result:v1.0' +
              '(by /u/blockedaccessible)'}
    unique_ident = get_unique_key(politics_url)

    # first, look in the cache to see if we already have this data
    if unique_ident in CACHE_DICTION:
        print("Getting cached data...")
        return CACHE_DICTION[unique_ident]

    # if not, fetch the data afresh, add it to the cache,
    # then write the cache to file
    else:
        print("Making a request for new data...")
        # Make the request and cache the new data
        resp = requests.get(politics_url, headers=header)
        CACHE_DICTION[unique_ident] = resp.text
        dumped_json_cache = json.dumps(CACHE_DICTION)
        fw = open(CACHE_FNAME, "w")
        fw.write(dumped_json_cache)
        fw.close()  # Close the open file
        return CACHE_DICTION[unique_ident]


def make_international_request_using_cache(world_politics_url):
    header = {'User-Agent': 'windows:r/worldpolitics.single.result:v1.0' +
              '(by /u/blockedaccessible)'}
    unique_ident = get_unique_key(world_politics_url)

    # first, look in the cache to see if we already have this data
    if unique_ident in CACHE_DICTION:
        print("Getting cached data...")
        return CACHE_DICTION[unique_ident]

    # if not, fetch the data afresh, add it to the cache,
    # then write the cache to file
    else:
        print("Making a request for new data...")
        # Make the request and cache the new data
        resp = requests.get(world_politics_url, headers=header)
        CACHE_DICTION[unique_ident] = resp.text
        dumped_json_cache = json.dumps(CACHE_DICTION)
        fw = open(CACHE_FNAME, "w")
        fw.write(dumped_json_cache)
        fw.close()  # Close the open file
        return CACHE_DICTION[unique_ident]


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
        DROP TABLE IF EXISTS 'Domestic';
    '''
    cur.execute(statement)

    statement = '''
        DROP TABLE IF EXISTS 'International';
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

    generate = '''
        CREATE TABLE 'Domestic' (
            'ID' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Headline' TEXT,
            'Alpha2' TEXT,
            'Polarity' REAL,
            'Upvote Score' INTEGER
        );
    '''
    cur.execute(generate)

    generate = '''
        CREATE TABLE 'International' (
            'ID' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Headline' TEXT,
            'Alpha2' TEXT,
            'Polarity' REAL,
            'Upvote Score' INTEGER
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


def get_politics_data():
    sia = SIA()
    headlines = []
    post_score = []
    polarity_score = []
    pos_list = []
    neg_list = []

    request = make_domestic_request_using_cache(politics_url)
    json_data = json.loads(request)
    data_all = json_data['data']['children']
    num_of_posts = 0
    while len(data_all) <= 1000:
        time.sleep(2)
        last = data_all[-1]['data']['name']
        url = 'https://www.reddit.com/r/politics/.json?after=' + str(last)
        req = make_domestic_request_using_cache(url)
        data = json.loads(req)
        data_all += data['data']['children']
        if num_of_posts == len(data_all):
            break
        else:
            num_of_posts = len(data_all)

    for post in data_all:
        headlines.append(post['data']['title'])
        post_score.append(post['data']['score'])
        res = sia.polarity_scores(post['data']['title'])
        polarity_score.append(res['compound'])
        if res['compound'] > 0.2:
            pos_list.append(post['data']['title'])
        elif res['compound'] < -0.2:
            neg_list.append(post['data']['title'])

    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except Error as e:
        print(e)

    # Domestic Table: ID, Headline, Alpha2, Polarity Score, Upvote Score

    for post in data_all:
        insertion = (None, post['data']['title'], None,
                     None, post['data']['score'])
        statement = 'INSERT INTO "Domestic" '
        statement += 'VALUES (?, ?, ?, ?, ?)'
        cur.execute(statement, insertion)

    for post in polarity_score:
        cur.execute('INSERT INTO Domestic (Polarity) VALUES (?)', (post,))

    conn.commit()
    conn.close()

def get_world_politics():
    sia = SIA()
    headlines = []
    post_score = []
    polarity_score = []
    pos_list = []
    neg_list = []

    request = make_international_request_using_cache(world_politics_url)
    json_data = json.loads(request)
    data_all = json_data['data']['children']
    num_of_posts = 0
    while len(data_all) <= 1000:
        time.sleep(2)
        last = data_all[-1]['data']['name']
        url = 'https://www.reddit.com/r/worldpolitics/.json?after=' + str(last)
        req = make_international_request_using_cache(url)
        data = json.loads(req)
        data_all += data['data']['children']
        if num_of_posts == len(data_all):
            break
        else:
            num_of_posts = len(data_all)

    for post in data_all:
        headlines.append(post['data']['title'])
        post_score.append(post['data']['score'])
        res = sia.polarity_scores(post['data']['title'])
        polarity_score.append(res['compound'])
        if res['compound'] > 0.2:
            pos_list.append(post['data']['title'])
        elif res['compound'] < -0.2:
            neg_list.append(post['data']['title'])
    try:
        conn = sqlite3.connect(DBNAME)
        cur = conn.cursor()
    except Error as e:
        print(e)

    # International Table: ID, Headline, Alpha2, Polarity Score, Upvote Score

    for post in data_all:
        insertion = (None, post['data']['title'], None,
                     None, post['data']['score'])
        statement = 'INSERT INTO "International" '
        statement += 'VALUES (?, ?, ?, ?, ?)'
        cur.execute(statement, insertion)

    for post in polarity_score:
        cur.execute('INSERT INTO International (Polarity) VALUES (?)', (post,))

    conn.commit()
    conn.close()
