import json
import pytz
import ujson
import datetime

undefined = object()

JSON_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def read(text):
    try:
        return ujson.decode(text)
    except Exception as e:
        raise DecodeError(e)


def read_number_list(text):
    return [int(i) for i in read(text)]


def write(obj, encode_date=undefined, ensure_ascii=True):
    if encode_date == undefined:
        encode_date = JSON_DATE_FORMAT
    try:
        return ujson.encode(obj, encode_date=encode_date, ensure_ascii=ensure_ascii)
    except Exception as e:
        raise EncodeError(e)


class EncodeError(ValueError):
    pass

class DecodeError(ValueError):
    pass


def dump_all(obj):
    """
    Dump any object to JSON.

    If object contains fields which cannot be converted to JSON,
    we do all our best to keep as much info as we can, and
    convert them with :func:`repr`.

    Currently used in testing with pytest, where render_template also sets up
    the header 'X-Parts-NS'
    """
    def json_default(obj):
        if hasattr(obj, '__json__'):
            return obj.__json__()
        return repr(obj)
    return json.dumps(obj, default=json_default, separators=",:")


def parse_date(str_date):
    """
    If `str_date` is a string then it's is formated with datetime.strptime (using the format that jsonist.write uses).
    It will throw a ValueError if the date can't be parsed using datetime.strptime.
    """
    result = None

    if not hasattr(str_date, 'strftime'):
        try:
            result = datetime.datetime.strptime(str_date, "%a %d %b %Y %H:%M:%S +0000")
        except ValueError:
            result = datetime.datetime.strptime(str_date, "%a %d %b %Y %H:%M:%S")

    if result:
        return result.replace(tzinfo=pytz.UTC)
    else:
        raise ValueError()
