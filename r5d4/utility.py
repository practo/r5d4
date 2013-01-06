from __future__ import absolute_import
from datetime import datetime, timedelta
from dateutil.parser import parse
from flask import jsonify


def fmt_date(date):
    return date.strftime("%Y%m%d")


def datetime_iterator(from_date=None, to_date=None, delta=timedelta(days=1)):
    from_date = from_date or datetime.now()
    to_date = to_date or datetime.now()
    if from_date > to_date:
        while from_date >= to_date:
            yield from_date
            from_date = from_date - delta
    else:
        while from_date <= to_date:
            yield from_date
            from_date = from_date + delta


def date_iterator(from_date, to_date):
    """
    Should Iterate through datetime and return formatted string
    >>> map(fmt_date, date_iterator(parse("01-Feb-2003"), parse("3-Feb-2003")))
    ['20030201', '20030202', '20030203']

    >>> map(fmt_date, date_iterator(parse("Jan 31 2011"), parse("Feb 2 2011")))
    ['20110131', '20110201', '20110202']

    >>> map(fmt_date, date_iterator(parse("Feb 2 2011"), parse("Jan 31 2011")))
    ['20110202', '20110201', '20110131']
    """
    return datetime_iterator(from_date, to_date, timedelta(days=1))


def week_iterator(from_date, to_date):
    """
    >>> map(fmt_date, week_iterator(parse("1-Sep-2011"), parse("14-Sep-2011")))
    ['20110829', '20110905', '20110912']
    """
    from_date = from_date - timedelta(days=from_date.weekday())
    to_date = to_date - timedelta(days=to_date.weekday())
    return datetime_iterator(from_date, to_date, timedelta(days=7))


def month_iterator(from_date, to_date):
    """
    >>> map(fmt_date, month_iterator(parse("15-11-2005"), parse("20-3-2006")))
    ['20051101', '20051201', '20060101', '20060201', '20060301']
    >>> map(fmt_date, month_iterator(parse("20-3-2006"), parse("15-11-2005")))
    ['20060301', '20060201', '20060101', '20051201', '20051101']
    """
    from_date = from_date.replace(day=1)
    to_date = to_date.replace(day=1)
    if from_date > to_date:
        while from_date >= to_date:
            yield from_date
            if from_date.month == 1:
                from_date = from_date.replace(year=from_date.year - 1,
                                              month=12)
            else:
                from_date = from_date.replace(month=from_date.month - 1)
    else:
        while from_date <= to_date:
            yield from_date
            if from_date.month == 12:
                from_date = from_date.replace(year=from_date.year + 1,
                                              month=1)
            else:
                from_date = from_date.replace(month=from_date.month + 1)


def year_iterator(from_date, to_date):
    """
    >>> map(fmt_date, year_iterator(parse("01-Feb-2003"), parse("3-Feb-2005")))
    ['20030101', '20040101', '20050101']
    """
    from_date = from_date.replace(day=1, month=1, tzinfo=None)
    to_date = to_date.replace(day=1, month=1, tzinfo=None)
    if from_date > to_date:
        while from_date >= to_date:
            yield from_date
            from_date = from_date.replace(year=from_date.year - 1)
    else:
        while from_date <= to_date:
            yield from_date
            from_date = from_date.replace(year=from_date.year + 1)


def json_response(f):
    def new_f(*args, **kwargs):
        return jsonify(f(*args, **kwargs))
    return new_f


def construct_key(*args):
    """
    >>> construct_key()
    ''

    >>> construct_key('Activity', [''])
    'Activity'

    >>> construct_key('Activity', ['Month', '20111101'], [])
    'Activity:Month:20111101'

    >>> construct_key('Activity', ['Month', '20111101'], ['Practice', 1])
    'Activity:Month:20111101:Practice:1'

    >>> construct_key('Activity', 'Month:20111101', 'Practice:1')
    'Activity:Month:20111101:Practice:1'

    >>> construct_key('Activity', ['Month', '20111101'], None)
    'Activity:Month:20111101'

    """
    def flatten_args(args):
        flattened = []
        for arg in args:
            if type(arg) == list or type(arg) == tuple:
                flattened.extend(flatten_args(arg))
            elif arg is None or str(arg) == '':
                continue
            else:
                flattened.append(str(arg))
        return flattened
    flattened_args = flatten_args(args)
    if flattened_args == []:
        return ''
    return reduce(lambda x, y: x + ':' + y, flattened_args)

if __name__ == "__main__":
    fmt_date(parse("Jan 31 2011"))  # suppress unused 'parse' warning
