import socket
import time
import sys
import datetime
import pickle
import ipaddress
import struct
import numpy as np

import fxp_bytes_subscriber as fxp

import bellman_ford

"""

Mark Taylor
Software is a simulated Arbitradge Trader using UDP/IP
connection to a subscriber. System simulates a PUB/SUB distributed
system.

Distributed Systems
10/16/21

"""

# TODOs
# Subscribe to Forex service X
# print each published message X


# Each published message update a graph based on pricing

# Run Bellman ford algo

# Report arbitrage opportunities


# Stores all bash arguments from cmd line, excluding python file name

MSG_LENGTH = 32


class subscriber(object):
    def __init__(self, host, port):
        print("subscriber")
        self.hostname = host
        self.hostport = port
        print("HOST: {}".format(self.hostname))

    def sendMsg(self):
        """
        Join publisher
        :return:
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(('localhost', 55555))
            IP4address = int(ipaddress.ip_address(self.hostname))
            byte_address = IP4address.to_bytes(4, "big")
            byte_port = self.hostport.to_bytes(2, "big")
            print("byte_address {}".format(byte_address))
            print("byte port {}".format(byte_port))
            byte_sub_msg = byte_address + byte_port
            sock.sendall(byte_sub_msg)

    def socketCreate(self):
        server_address = ('localhost', 54444)
        print('starting up on {} port {}'.format(*server_address))

        # Create a UDP socket
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            # subscriber binds the socket to the publishers address
            sock.settimeout(5)
            sock.bind(server_address)
            while True:
                print('\nblocking, waiting to receive message')
                try:
                    data, addr = sock.recvfrom(4096)
                    print("Message received!")
                    # print("DATA LENGTH: {}".format(len(data)))
                    chunks = int(len(data) / MSG_LENGTH)
                    print("NUMBER OF QUOTES IN BLOCK: {}".format(chunks))
                    quotes = self.split(data, chunks)

                    for data in quotes:

                        # number of ms passed since 00:00:00 UTC
                        timestamp = fxp.getMs(data[0:8])
                        print("TimeStamp: {}".format(timestamp))

                        currency = fxp.getCurrency(data[8:14])
                        print("Currency: {}".format(currency))

                        try:
                            exchRate = fxp.getExchangeRate(data[14:22])
                            print("Exchange Rate: {}".format(exchRate))
                        except Exception as e:
                            print(e)
                            print(len(data[14:22]))
                        # struct.error: unpack requires a buffer of 8 bytes
                        reserved = fxp.getReserved(data[22:32])
                        print("reserved: {}".format(reserved))

                    print("BREAKER----------------------------")
                except Exception as e:
                    print(e)
                    self.sendMsg()

    @staticmethod
    def split(a, n):
        k, m = divmod(len(a), n)
        return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in
                range(n))

    def run(self):
        print("running")
        sub.sendMsg()
        sub.socketCreate()
        print("send join req")


if __name__ == '__main__':
    sub = subscriber('127.0.0.1', 54444)
    sub.run()

    """
    args = sys.argv[1:]
    
    if len(args) >= 2:

        host_address = ""
        sub = subscriber(host_address)
        sub.run()
    else:
        print("Did not receive a proper input")
        exit(1)
    """
