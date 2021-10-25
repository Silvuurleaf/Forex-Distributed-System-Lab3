import socket
import time
import sys
from datetime import datetime
import pickle
import ipaddress
import struct
import math

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
INITIAL_INVESTMENT = 100


class subscriber(object):
    def __init__(self, host, port):
        print("subscriber")
        self.hostname = host
        self.hostport = port
        print("HOST: {}".format(self.hostname))
        self.coinbase = bellman_ford.Graph()
        self.time = datetime.now()

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

                        if timestamp >= self.time:

                            self.time = timestamp

                            # get currency pair
                            currency = fxp.getCurrency(data[8:14])

                            node = currency[0:3]
                            neighbor = currency[3:6]

                            exchRate = fxp.getExchangeRate(data[14:22])
                            print("{}: {} to {}: {}".
                                      format(timestamp, node, neighbor, exchRate))

                            # struct.error: unpack requires a buffer of 8 bytes
                            # reserved = fxp.getReserved(data[22:32])

                            self.add_nodes_toGraph(node, neighbor, exchRate)

                        else:
                            print("--------STALE QUOTE!-------------------")

                    print("\n-----------------NEW QUOTE PACKET----------------")
                except Exception as e:
                    print(e)
                    # connection was lost re-establish connection
                    self.sendMsg()

                #currency = self.coinbase.vertices[0]
                currency = 'USD'
                arbitragePath = self.coinbase.bellman_ford(currency)

                if arbitragePath:
                    self.proccessPath(arbitragePath, currency)


    def proccessPath(self, path, startnode):

        print("SOURCE NODE: {}".format(startnode))
        print("ARBITRAGE PATH PREPROCESS: {}".format(path))
        try:
            # KEY = SUCCESSOR
            # VALUE = PARENT
            START = startnode
            parentNode = path[START]
            arbitragePath = []
            while startnode != parentNode:
                arbitragePath.append(parentNode)
                parentNode = path[parentNode]


            print("ARBITRAGE PATH: {}".format(arbitragePath))

            arbitragePath.append(START)
            arbitragePath.reverse()
            arbitragePath.append(START)
            print("ARBITRAGE PATH: {}".format(arbitragePath))

            accumulated = INITIAL_INVESTMENT
            for i in range(len(arbitragePath)):

                if i+1 != len(arbitragePath):
                    parentToken = arbitragePath[i]
                    successorToken = arbitragePath[i+1]

                    edge = self.coinbase.graph[parentToken][successorToken]
                    rate = 10 ** (-1 * edge)
                    totalPast = accumulated
                    accumulated = accumulated * rate
                    print("{} of {} exchanged to {} at rate: {} = {}".format(
                        totalPast,
                        parentToken, successorToken,
                        rate,
                        accumulated))
        except Exception as e:
            print(e)
            print("Arbitrage source node keyerror")


    def add_nodes_toGraph(self, node, neighbor, exchangeRate):
        self.coinbase.add_node(node)
        self.coinbase.add_edge(node, neighbor, exchangeRate)

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

