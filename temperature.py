"""Module Downloads Data and Creates Weather Database """
import requests
import os
import datetime
import time
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

CITIES = {
    'Los_Angeles': '34.019394,-118.410825',
    'Austin': '30.303936,-97.754355',
    'Denver': '39.761850,-104.881105',
    'New_Orleans': '30.053420,-89.934502',
    'New_York': '40.663619,-73.938589'
}

URL = 'https://api.forecast.io/forecast/{}/{},{}'

NOW = datetime.datetime.now()

START_TIME = NOW - datetime.timedelta(days=30)

def create_db():
    """Create database to store max_temp values"""

    conn = sqlite3.connect('weather.db')
    curs = conn.cursor()
    curs.execute('DROP TABLE if EXISTS max_temp;')
    curs.execute('CREATE TABLE max_temp (id INTEGER PRIMARY KEY AUTOINCREMENT,'
                 ' time TEXT, Los_Angeles NUMERIC, Austin NUMERIC, Denver NUMERIC,'
                 ' New_Orleans NUMERIC, New_York NUMERIC)')
    query_time = START_TIME
    while query_time <= NOW:
        curs.execute('INSERT INTO max_temp (time) VALUES (?)',
                     (query_time.isoformat(),))
        query_time += datetime.timedelta(days=1)
    conn.commit()
    conn.close()

def get_weather():
    """Make API calls to forecast.io for max_temp"""

    api_key = os.environ.get('FORECAST_API_KEY', '')
    if api_key == '':
        print ("You need to register and get an API KEY at "
               "https://developer.forecast.io/register before you can run"
               "this script! Export the API KEY into your terminal's "
               "environment and then run again.")
        return

    conn = sqlite3.connect('weather.db')
    curs = conn.cursor()

    for city, latlon in CITIES.iteritems():
        print "Downloading data for {}".format(city)
        query_time = START_TIME
        counter = 1
        while query_time <= NOW:
            time_str = str(int(time.mktime(query_time.timetuple())))
            url = URL.format(api_key, latlon, time_str)
            req = requests.get(url)
            mtemp = req.json()['daily']['data'][0]['temperatureMax']
            with conn:
                curs.execute('UPDATE max_temp SET ' + city + ' = ' +
                             str(mtemp) + ' WHERE time = "' +
                             query_time.isoformat() + '";')
            print "Updated Table {} time(s) for {}".format(str(counter), city)
            counter += 1
            query_time += datetime.timedelta(days=1)
    conn.commit()
    conn.close()

def analyze_weather():
    """ Queries Database and summarizes info about Max Temp """
    conn = sqlite3.connect('weather.db')
    data_frame = pd.read_sql_query('SELECT time, Los_Angeles, Austin, Denver, '
                                   'New_Orleans, New_York from max_temp order '
                                   'by time', conn, index_col='time')
    means = data_frame.mean()
    print 'Means:'
    print means

    medians = data_frame.median()
    print '\nMedians:'
    print medians

    variances = data_frame.var()
    print '\nVariances:'
    print variances

    ranges = data_frame.max() - data_frame.min()
    print '\nRanges:'
    print ranges

    plt.figure()
    pd.scatter_matrix(data_frame, diagonal='kde')
    plt.suptitle('Scatter Plot of Max Temps of 5 Cities')
    plt.savefig('temp_scatter.png')
    print "\nSee scatterplots of the max temps in 'temp_scatter.png'"
    plt.close()

def analyze_diff():
    """Analyzes the Changes in Max Temp by City"""
    conn = sqlite3.connect('weather.db')
    data_frame = pd.read_sql_query('SELECT time, Los_Angeles, Austin, Denver, '
                                   'New_Orleans, New_York from max_temp order '
                                   'by time', conn, index_col='time')
    diffed = data_frame.diff().dropna()
    fig = plt.figure()
    fig.subplots_adjust(hspace=0.8)
    fig.suptitle('Distribution of Temp. Changes by City')
    for idx, title in enumerate(CITIES.keys()):
        sub = 321 + idx
        plt.subplot(sub)
        plt.hist(diffed[title])
        plt.title(title)
        plt.xlabel('Change in Max Temp')
        plt.ylabel('Frequency')
    plt.avefig('diffed_temps.png')
    print "\nSee histograms of the diffed temps at 'diffed_temps.png'\n"
    plt.close()

if __name__ == '__main__':
    if not os.path.isfile('weather.db'):
        create_db()
        get_weather()
    analyze_weather()
    analyze_diff()
