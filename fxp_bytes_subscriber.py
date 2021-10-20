"""
Unmarshalling for subscriber

"""
import struct


def getMs(data: bytes):
    return int.from_bytes(data, byteorder='big', signed=False)


def getExchangeRate(data: bytes):
    return struct.unpack('d', data)[0] #(1.11111, )


def getCurrency(data: bytes):
    return data.decode("utf-8")


def getReserved(data: bytes):
    return int.from_bytes(data[22:32], byteorder='big', signed=False)
