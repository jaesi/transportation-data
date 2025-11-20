from datetime import datetime, date, timedelta

def date_range(s: date, e: date):
    d = s
    while d <= e:
        yield d
        d += timedelta(days=1)