"""
Unmarshalling for subscriber

"""
import struct
from datetime import datetime, timezone

MICROS_PER_SECOND = 1_000_000

def getMs(data: bytes):

    microSeconds = int.from_bytes(data, "big")

    timeSeconds = microSeconds / MICROS_PER_SECOND
    timeIn = datetime.fromtimestamp(timeSeconds, tz=timezone.utc)

    return timeIn


def getExchangeRate(data: bytes):
    return struct.unpack('d', data)[0] #(1.11111, )


def getCurrency(data: bytes):
    return data.decode("utf-8")


def getReserved(data: bytes):
    return int.from_bytes(data[22:32], byteorder='big', signed=False)
