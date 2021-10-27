"""
Unmarshalling for subscriber

"""
import struct
from datetime import datetime, timezone

MICROS_PER_SECOND = 1_000_000

def getMs(data: bytes):
    """
    Take sin byte data and processes it into UTC datetime
    :param data: byte message
    :return: timestamp in UTC format
    """

    microSeconds = int.from_bytes(data, "big")

    timeSeconds = microSeconds / MICROS_PER_SECOND
    timeIn = datetime.fromtimestamp(timeSeconds, tz=timezone.utc)

    return timeIn


def getExchangeRate(data: bytes):
    """
    unpacks the exchange rate as a 64 bit float
    :param data: byte message
    :return: 64 bit float
    """
    return struct.unpack('d', data)[0] #(1.11111, )


def getCurrency(data: bytes):
    """
    Gets the currency information
    :param data: byte message
    :return: String representing the currency pair
    """
    return data.decode("utf-8")


def getReserved(data: bytes):
    """
    Gets the last bits of a the message.
    :param data: byte message
    :return: Empty list of 0, for reserved data unused for now
    """
    return int.from_bytes(data[22:32], byteorder='big', signed=False)
