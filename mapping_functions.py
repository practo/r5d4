from __future__ import absolute_import
from dateutil.parser import parse
from datetime import timedelta
from r5d4.utility import (date_iterator, week_iterator, month_iterator,
                          year_iterator)


# Measuring functions

def score(conn, tr_type, **kwargs):
    key_str = kwargs["key_str"]
    field_val = kwargs["field_val"]

    if tr_type.lower() == "insert":
        return conn.incr(key_str, field_val)
    elif tr_type.lower() == "delete":
        return conn.decr(key_str, field_val)
    else:
        raise ValueError("Unknown transaction type", tr_type)


def score_float(conn, tr_type, **kwargs):
    key_str = kwargs["key_str"]
    field_val = kwargs["field_val"]

    if tr_type.lower() == "insert":
        def incrbyfloat(pipe):
            current_value = pipe.get(key_str)
            if current_value is None:
                current_value = 0.0
            new_value = float(current_value) + field_val
            pipe.multi()
            pipe.set(key_str, new_value)
        return conn.transaction(incrbyfloat, key_str)
    elif tr_type.lower() == "delete":
        def decrbyfloat(pipe):
            current_value = pipe.get(key_str)
            if current_value is None:
                current_value = 0.0
            new_value = float(current_value) - field_val
            pipe.multi()
            pipe.set(key_str, new_value)
        return conn.transaction(decrbyfloat, key_str)
    else:
        raise ValueError("Unknown transaction type", tr_type)


def count(conn, tr_type, **kwargs):
    kwargs["field_val"] = 1
    return score(conn, tr_type, **kwargs)


def count_float(conn, tr_type, **kwargs):
    kwargs["field_val"] = 1.0
    return score_float(conn, tr_type, **kwargs)


def heat(conn, tr_type, **kwargs):
    return count(conn, "insert", **kwargs)


def heat_float(conn, tr_type, **kwargs):
    return count_float(conn, "insert", **kwargs)


def unique(conn, tr_type, **kwargs):
    key_str = kwargs["key_str"]
    field_val = kwargs["field_val"]
    conn.sadd(key_str, field_val)
    return conn.scard(key_str)

MEASURING_FUNCTIONS_MAP = {
    "heat": heat,
    "count": count,
    "score": score,
    "unique": unique,
    "heat_float": heat_float,
    "count_float": count_float,
    "score_float": score_float
}


CONDITION_KEYS = ["equals", "not_equals"]

# Dimension Parsing functions


def parse_integer(val):
    return int(val)


def parse_string(val):
    """
    Generic string type with no special powers.
    Shall take any character except ':' as it is used as
    the field delimiter in the keys.

    >>> parse_string(1)
    '1'

    >>> parse_string("alpha")
    'alpha'

    >>> parse_string("  sparse string ")
    'sparse string'

    >>> parse_string("some:text:with:colons")
    Traceback (most recent call last):
        ...
    ValueError: invalid value for string ('some:text:with:colons'), \
':' is not allowed
    """
    val = str(val)
    if ':' in val:
        raise ValueError(
            "invalid value for string ('%s'), ':' is not allowed" % val)
    return val.strip()


def parse_date_to_obj(date_str):
    try:
        return parse(date_str)
    except ValueError:
        raise ValueError("Invalid date", date_str)


def fmt_date(date):
    return date.strftime("%Y%m%d")


def parse_date(date_str):
    """
    Extract date part from given input string
    Input: Any date/Timestamp string format known to dateutil
    Output format: YYYYMMDD

    >>> parse_date("20111021")
    '20111021'

    >>> parse_date("2011-02-01 10:02:00")
    '20110201'

    >>> parse_date("guess me!")
    Traceback (most recent call last):
        ...
    ValueError: ('Invalid date', 'guess me!')

    >>> parse_date("2011-02-29 10:30:00")
    Traceback (most recent call last):
        ...
    ValueError: ('Invalid date', '2011-02-29 10:30:00')

    >>> parse_date(None)
    Traceback (most recent call last):
        ...
    ValueError: ('Invalid date', None)

    >>> parse_date("")
    Traceback (most recent call last):
        ...
    ValueError: ('Invalid date', '')

    """
    if date_str is None or date_str == "":
        raise ValueError('Invalid date', date_str)
    return fmt_date(parse_date_to_obj(date_str))


def parse_week(date_str):
    """
    >>> parse_week('21-Sep-2011')
    '20110919'

    >>> parse_week('19/9/2011')
    '20110919'
    """
    if date_str is None or date_str == "":
        raise ValueError('Invalid date', date_str)
    date_obj = parse_date_to_obj(date_str)
    return fmt_date(date_obj - timedelta(days=date_obj.weekday()))


def parse_month(date_str):
    """
    >>> parse_month('1-Feb-2011')
    '20110201'

    >>> parse_month('29-Feb-2011')
    Traceback (most recent call last):
        ...
    ValueError: ('Invalid date', '29-Feb-2011')

    >>> parse_month('2/2011')
    '20110201'

    >>> parse_month('23/2/2011')
    '20110201'
    """
    if date_str is None or date_str == "":
        raise ValueError('Invalid date', date_str)
    return fmt_date(parse_date_to_obj(date_str).replace(day=1))


def parse_year(date_str):
    """
    >>> parse_year('1-Feb-2011')
    '20110101'

    >>> parse_year('2002')
    '20020101'

    >>> parse_year('Wed Sep 21 10:27:58 UTC 2011')
    '20110101'
    """
    if date_str is None or date_str == "":
        raise ValueError('Invalid date', date_str)
    return fmt_date(parse_date_to_obj(date_str).replace(day=1, month=1))


DIMENSION_PARSERS_MAP = {
    "integer": parse_integer,
    "string": parse_string,
    "date": parse_date,
    "week": parse_week,
    "month": parse_month,
    "year": parse_year
}


RANGE_OPERATOR = '..'


def expand_integer(str_range):
    """
    >>> expand_integer('1') == set(['1'])
    True

    >>> expand_integer('1..5,10') == set(['1','2','3','4','5','10'])
    True

    >>> expand_integer('9..3') == set(['3','4','5','6','7','8','9'])
    True

    >>> expand_integer('1..5,8..3') == set(['1','2','3','4','5','6','7','8'])
    True

    >>> expand_integer('try me')
    Traceback (most recent call last):
        ...
    ValueError: integer range 'try me' not parseable

    >>> expand_integer('1..a')
    Traceback (most recent call last):
        ...
    ValueError: integer range '1..a' not parseable

    """
    try:
        answer = set()
        for i_group in str_range.split(','):
            if RANGE_OPERATOR in i_group:
                i_start, i_end = i_group.split(RANGE_OPERATOR)
                if i_start > i_end:
                    i_start, i_end = i_end, i_start
                start_int = parse_integer(i_start)
                end_int = parse_integer(i_end)
                answer |= set(map(str, range(start_int, end_int + 1)))
            else:
                answer.add(str(parse_integer(i_group)))
        return answer
    except ValueError:
        raise ValueError("integer range '%s' not parseable" % str_range)


def expand_string(range_str):
    """
    >>> expand_string('a,b,c') == set(['a','b','c'])
    True

    >>> expand_string('alpha, beta, gamma') == set(['alpha', 'beta', 'gamma'])
    True

    >>> expand_string('try me') == set(['try me'])
    True

    >>> expand_string('ihave:colon, innocent')
    Traceback (most recent call last):
        ...
    ValueError: invalid value for string ('ihave:colon'), ':' is not allowed

    >>> expand_string('a..z')
    Traceback (most recent call last):
        ...
    ValueError: range operator is not supported for string ('a..z')

    """
    if RANGE_OPERATOR in range_str:
        raise ValueError("range operator is not supported for string ('%s')" %
                         range_str)
    return set(map(parse_string, range_str.split(',')))


def expand_date_family(range_str, parse, iterator):
    answer = set()
    for d_group in range_str.split(','):
        if RANGE_OPERATOR in d_group:
            d_start, d_end = map(parse_date_to_obj,
                                 d_group.split(RANGE_OPERATOR))
            answer |= set(map(fmt_date, iterator(d_start, d_end)))
        else:
            answer.add(parse(d_group))
    return answer


def expand_date(range_str):
    """
    >>> expand_date('Aug-1 2011') == set(['20110801'])
    True

    >>> expand_date('20110709..20110712') == \
set(['20110709', '20110710', '20110711', '20110712'])
    True

    >>> expand_date('20110801..Aug-2-2011,2011-8-4..2011-8-2') == \
set(['20110801', '20110802', '20110803', '20110804'])
    True

    >>> expand_date('20110228..20110302') == \
set(['20110228', '20110301', '20110302'])
    True

    >>> expand_date('20110230')
    Traceback (most recent call last):
        ...
    ValueError: ('Invalid date', '20110230')
    """
    return expand_date_family(range_str, parse_date, date_iterator)


def expand_week(range_str):
    """
    >>> expand_week('Sep-1 2011') == set(['20110829'])
    True

    >>> expand_week('20110901..20110914') == \
set(['20110829', '20110905', '20110912'])
    True
    """
    return expand_date_family(range_str, parse_week, week_iterator)


def expand_month(range_str):
    """
    >>> expand_month('Sep-2011..Feb-2012') == \
set(['20110901', '20111001', '20111101', '20111201', '20120101', '20120201'])
    True
    """
    return expand_date_family(range_str, parse_month, month_iterator)


def expand_year(range_str):
    """
    >>> expand_year('Wed Sep 21 10:27:58 UTC 2011..2009') == \
set(['20090101', '20100101', '20110101'])
    True

    >>> expand_year('2011..2014') == \
set(['20110101', '20120101', '20130101', '20140101'])
    True
    """
    return expand_date_family(range_str, parse_year, year_iterator)

DIMENSION_EXPANSION_MAP = {
    "integer": expand_integer,
    "string": expand_string,
    "date": expand_date,
    "week": expand_week,
    "month": expand_month,
    "year": expand_year
}
