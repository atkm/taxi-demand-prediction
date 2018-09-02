import json
import time
import datetime
from urllib.parse import urlencode
from urllib.request import urlopen

# Number of attempts to download data
MAX_ATTEMPTS = 6
# HTTPS here can be problematic for installs that don't have Lets Encrypt CA
SERVICE = "http://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?"


def download_data(uri):
    """Fetch the data from the IEM

    The IEM download service has some protections in place to keep the number
    of inbound requests in check.  This function implements an exponential
    backoff to keep individual downloads from erroring.

    Args:
      uri (string): URL to fetch

    Returns:
      string data
    """
    attempt = 0
    while attempt < MAX_ATTEMPTS:
        try:
            data = urlopen(uri, timeout=300).read().decode('utf-8')
            if data is not None and not data.startswith('ERROR'):
                return data
        except Exception as exp:
            print("download_data(%s) failed with %s" % (uri, exp))
            time.sleep(5)
        attempt += 1

    print("Exhausted attempts to download, returning empty data")
    return ""


def get_lga(year, month):
    """
    Get data for LGA month-by-month.
    Sample query string:
    https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py?station=LGA&data=all&year1=2014&month1=1&day1=1&year2=2014&month2=2&day2=1&tz=America%2FNew_York&format=onlycomma&latlon=no&direct=no&report_type=2
    """
    assert 1 <= month and month <= 12, "Invalid month"
    assert 2009 <= year and year <= 2017, "Invalid year"

    # timestamps in UTC to request data for
    startts = datetime.datetime(year, month, 1)
    endts = datetime.datetime(year, month, 1)

    uri = SERVICE

    uri += startts.strftime('year1=%Y&month1=%m&day1=%d&')
    uri += endts.strftime('year2=%Y&month2=%m&day2=%d&')

    query_dict = {
            'data': 'all',
            'station': 'LGA',
            'latlon': 'no',
            'report_type': '2',
            'format': 'onlycomma', #'comma' to include DEBUG header
            'tz': 'America/New_York',
            }

    uri += urlencode(query_dict)

    ym = startts.strftime("%Y-%m")

    print(f'Downloading: {ym}...', end='')
    data = download_data(uri)
    out_file = f'lga_{ym}.csv'
    with open(out_file, 'w') as f:
        f.write(data)
    print('Done.')

if __name__ == '__main__':
    for y in range(2010,2018):
        for m in range(1,13):
            get_lga(y, m)
