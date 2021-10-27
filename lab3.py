import socket
import time
import sys
from datetime import datetime, timezone
import pickle
import ipaddress
import struct
import math

import fxp_bytes_subscriber as fxp

import bellman_ford

"""

Mark Taylor
Software is a simulated Arbitrage Trader using UDP/IP
connection to a subscriber. System simulates a PUB/SUB distributed
system.

Distributed Systems
10/16/21

"""


MSG_LENGTH = 32
INITIAL_INVESTMENT = 100


class subscriber(object):
    def __init__(self, host, port):
        print("subscriber")
        self.hostname = host
        self.hostport = port
        print("HOST: {}".format(self.hostname))

        self.coinbase = bellman_ford.Graph()
        self.time = 0  # datetime.utcnow().timestamp()

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
                print('\nBLOCKING -- waiting to receive message')
                try:
                    data, addr = sock.recvfrom(4096)
                    print("--------MESSAGE RECEIVED!--------")
                    chunks = int(len(data) / MSG_LENGTH)
                    print("NUMBER OF QUOTES IN BLOCK: {}".format(chunks))

                    quotes = self.split(data, chunks)

                    for data in quotes:
                        self.processQuoteData(data)

                    print("\n--------NEW QUOTE PACKET--------")

                except Exception as e:
                    print(e)

                    # connection was lost re-establish connection
                    self.sendMsg()

                # currency = self.coinbase.vertices[0]
                currency = 'USD'
                arbitragePath = self.coinbase.bellman_ford(currency)

                if arbitragePath:
                    self.processPath(arbitragePath, currency)

                # check for stale timestamps
                now = datetime.now(tz=timezone.utc)
                staleQuotes = self.coinbase.checkStale(now)
                if staleQuotes:
                    for quote in staleQuotes:
                        print(quote)

    def processQuoteData(self, data):

        # number of ms passed since 00:00:00 UTC
        timestamp = fxp.getMs(data[0:8])
        # timestamp in seconds
        timestampSeconds = timestamp.timestamp()

        if timestampSeconds >= self.time:

            self.time = timestampSeconds

            # get currency pair
            currency = fxp.getCurrency(data[8:14])
            node = currency[0:3]
            neighbor = currency[3:6]

            exchRate = fxp.getExchangeRate(data[14:22])
            print("{}: {} to {}: {}".
                  format(timestamp, node, neighbor, exchRate))

            # struct.error: unpack requires a buffer of 8 bytes
            # reserved = fxp.getReserved(data[22:32])

            self.add_nodes_toGraph(node, neighbor,
                                   exchRate, timestampSeconds)

        else:
            print("--------IGNORING OUT OF SEQUENCE QUOTE!--------")

    def processPath(self, path, startnode):

        print("SOURCE NODE: {}".format(startnode))
        print("ARBITRAGE PATH PREPROCESS: {}".format(path))

        try:
            # KEY = SUCCESSOR
            # VALUE = PARENT

            START = startnode
            parentNode = path[START]
            # Path = {USD: JPY, JPY:CAD, CAD: GBP, GBP:USD}
            arbitragePath = []

            # visited array []
            while START != parentNode:
                arbitragePath.append(parentNode)
                parentNode = path[parentNode]

            # [USD JPY CAD GBP USD] backwards order
            # [USD GBP CAD JPY USD] canonical order

            print("ARBITRAGE PATH: {}".format(arbitragePath))
            # [JPY CAD GBP, USD]
            arbitragePath.append(START)

            # [USD, GBP, CAD, JPY]
            arbitragePath.reverse()

            # [USD, GBP, CAD, JPY, USD]
            arbitragePath.append(START)
            print("ARBITRAGE PATH: {}".format(arbitragePath))

            accumulated = INITIAL_INVESTMENT
            for i in range(len(arbitragePath)):

                if i + 1 != len(arbitragePath):

                    parentToken = arbitragePath[i]
                    successorToken = arbitragePath[i + 1]

                    edge = self.coinbase.graph[parentToken][successorToken][0]
                    rate = 10 ** (-1 * edge)
                    totalPast = accumulated
                    accumulated = accumulated * rate

                    self.printExchange(totalPast, parentToken,
                                       successorToken, rate,
                                       accumulated)
        except Exception as e:
            print(e)
            print("Arbitrage source node key-error.\n"
                  "Please be patient as the system resets.")

    def add_nodes_toGraph(self, node, neighbor, exchangeRate, timestamp):
        self.coinbase.add_node(node)
        self.coinbase.add_edge(node, neighbor, exchangeRate, timestamp)

    @staticmethod
    def printExchange(totalPast, parentToken, successorToken, rate, accumulated):
        print("{} of {} exchanged to {} at rate: {} = {}".format(
            totalPast,
            parentToken, successorToken,
            rate,
            accumulated))

    @staticmethod
    def split(a, n):
        k, m = divmod(len(a), n)
        return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in
                range(n))

    def run(self):
        print("running")
        sub.sendMsg()
        sub.socketCreate()
        print("Sending a publisher join request...")


if __name__ == '__main__':
    sub = subscriber('127.0.0.1', 54444)
    sub.run()
