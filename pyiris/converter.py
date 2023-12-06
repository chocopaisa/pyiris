import datetime
from decimal import Decimal
import time
from typing import (
    Any, Sequence, Union
)
from .error import ProgrammingError

def convert(val:Any):
    encoders = encoders_map.get(type(val))
    if not encoders:
        try:
            encoders = encoders_map[str]
        except KeyError:
            raise TypeError("No default type converter defined")
    return encoders(val)


def convert_dict(val:dict):
    n = {}
    for k, v in val.items():
        quoted = convert(v)
        n[k] = quoted
    return n


def convert_sequence(val:Sequence[Any]) -> str:
    n = []
    for item in val:
        quoted = convert(item)
        n.append(quoted)
    return "(" + ",".join(n) + ")"


def convert_set(val:set):
    return ",".join([convert(x) for x in val])


def convert_bool(value:bool):
    return str(int(value))

def convert_int(value:int):
    return str(value)

def convert_float(value:float):
    s = repr(value)
    if s in ("inf", "-inf", "nan"):
        raise ProgrammingError("%s can not be used with IRIS DB" % s)
    # if "e" not in s:
    #     s += "e0"
    return s


_table = [chr(x) for x in range(128)]
_table[0] = "\\0"
_table[ord("\\")] = "\\\\"
_table[ord("\n")] = "\\n"
_table[ord("\r")] = "\\r"
_table[ord("\032")] = "\\Z"
_table[ord('"')] = '\\"'
_table[ord("'")] = "\\'"



def convert_bytes(value:bytes) -> str:
    return convert_str(value.decode())

def convert_str(value:Union[str,Any]) -> str:
    # converted = value.translate(_table)
    # return "'%s'" % converted
    return "'%s'" % value

def convert_none(value:None) -> str:
    return "NULL"

def convert_timedelta(obj:datetime.timedelta) -> str:
    seconds = int(obj.seconds) % 60
    minutes = int(obj.seconds // 60) % 60
    hours = int(obj.seconds // 3600) % 24 + int(obj.days) * 24
    if obj.microseconds:
        fmt = "'{0:02d}:{1:02d}:{2:02d}.{3:06d}'"
    else:
        fmt = "'{0:02d}:{1:02d}:{2:02d}'"
    return fmt.format(hours, minutes, seconds, obj.microseconds)

def convert_time(obj:datetime.time) -> str:
    if obj.microsecond:
        fmt = "'{0.hour:02}:{0.minute:02}:{0.second:02}.{0.microsecond:06}'"
    else:
        fmt = "'{0.hour:02}:{0.minute:02}:{0.second:02}'"
    return fmt.format(obj)


def convert_datetime(obj:datetime.datetime) -> str:
    if obj.microsecond:
        fmt = (
            "'{0.year:04}-{0.month:02}-{0.day:02}"
            + " {0.hour:02}:{0.minute:02}:{0.second:02}.{0.microsecond:06}'"
        )
    else:
        fmt = "'{0.year:04}-{0.month:02}-{0.day:02} {0.hour:02}:{0.minute:02}:{0.second:02}'"
    return fmt.format(obj)


def convert_date(obj:datetime.date) -> str:
    fmt = "'{0.year:04}-{0.month:02}-{0.day:02}'"
    return fmt.format(obj)


def convert_struct_time(obj:time.struct_time) -> str:
    return convert_datetime(datetime.datetime(*obj[:6]))


def convert_decimal(obj:Decimal) -> str:
    return format(obj, "f")


encoders_map = {
    bool: convert_bool,
    int: convert_int,
    float: convert_float,
    str: convert_str,
    bytes: convert_bytes,
    tuple: convert_sequence,
    list: convert_sequence,
    set: convert_sequence,
    frozenset: convert_sequence,
    dict: convert_dict,
    type(None): convert_none,
    datetime.date: convert_date,
    datetime.datetime: convert_datetime,
    datetime.timedelta: convert_timedelta,
    datetime.time: convert_time,
    time.struct_time: convert_struct_time,
    Decimal: convert_decimal,
}