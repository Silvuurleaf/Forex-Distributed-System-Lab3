import socket
from datetime import datetime, timezone
import ipaddress

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
SOURCE_CURRENCY = "USD"


class subscriber(object):
    def __init__(self, host, port):
        """
        Initialize a subscriber object that will connect via
        UDP/IP to a provider and receive forex messages.
        :param host: hostname for our subscriber
        :param port: port number for our subscriber
        """

        print("----CREATING SUBSCRIBER---")
        self.hostname = host  # hostname for subscriber
        self.hostport = port  # port number for subscriber

        # Create graph class from bellman ford file
        self.coinbase = bellman_ford.Graph()

        # set current time to 0
        self.time = 0

    def joinPublisher(self):
        """
        Sends a request message to connect to the publisher
        and be added to its subscriber list.
        """

        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(('localhost', 50403))
            IP4address = int(ipaddress.ip_address(self.hostname))

            # convert address and port to big endian
            byte_address = IP4address.to_bytes(4, "big")
            byte_port = self.hostport.to_bytes(2, "big")

            # print("byte_address {}".format(byte_address))
            # print("byte port {}".format(byte_port))
            # Combine byte information and send to publisher
            byte_sub_msg = byte_address + byte_port
            sock.sendall(byte_sub_msg)

    def subscriberListen(self):
        """
        Creates a UDP listening server (subscriber) that awaits
        messages from a provider that delivers forex market messages.

        Messages are then unmarshalled and used to update a graph
        of currencies. Bellman Ford algorithm is then applied to
        the graph to find arbitrage opportunities.

        all information is printed out to the console
        """
        server_address = ('localhost', 54444)
        print('starting up on {} port {}'.format(*server_address))

        # Create a UDP socket to act as a listening server
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:

            # subscriber binds the socket to the publishers address
            sock.settimeout(5)
            sock.bind(server_address)

            # loop receives data from subscriber and processes it
            while True:

                print('\n--------BLOCKING WAITING FOR DATA--------')

                try:
                    data, addr = sock.recvfrom(4096)
                    print("--------MESSAGE RECEIVED!--------")

                    chunks = int(len(data) / MSG_LENGTH) #32
                    # print("NUMBER OF QUOTES IN BLOCK: {}\n".format(chunks))

                    # splits data into 32 bit chunks
                    quotes = self.split(data, chunks)

                    # process each 32 bit quote
                    for data in quotes:
                        self.processQuoteData(data)

                    print("\n--------NEW QUOTE PACKET--------\n")

                except Exception as e:
                    print(e)
                    # connection was lost re-establish connection
                    self.joinPublisher()

                # applies the bellman ford algorithm to find shortest paths
                # and negative cycles
                arbitragePath = self.coinbase.bellman_ford(SOURCE_CURRENCY)

                # checks if an arbitrage path was found and processes it
                if arbitragePath:
                    self.processPath(arbitragePath, SOURCE_CURRENCY)

                # checks graph for stale quotes
                self.checkForStaleQuotes()

    def checkForStaleQuotes(self):
        """
        Using UTC datetime checks graph for stale quotes
        and the graph class removes them.

        Stale is consider any edge that has been alive for
        greater than 1.5 seconds.
        """

        now = datetime.now(tz=timezone.utc)
        staleQuotes = self.coinbase.checkStale(now)
        # print out list of stale currency pairs
        if staleQuotes:
            print("\n--------CHECK FOR STALE QUOTES--------")
            for quote in staleQuotes:
                print(quote)

    def processQuoteData(self, data):
        """
        unmarshalls 32 bit data quote into three pieces
            1. Currency pair
            2. timestamp for the quote
            3. exchange rate between the pair
        Also checks to see if the message came in oder if not its
        ignored.

        :param data: 32 byte message to be decoded by the fxp class
        """
        # get UTC time from message
        timestamp = fxp.getMs(data[0:8])

        # timestamp in seconds
        timestampSeconds = timestamp.timestamp()

        # As long as the message is newer it can be used
        if timestampSeconds >= self.time:

            self.time = timestampSeconds

            # get currency pair
            currency = fxp.getCurrency(data[8:14])
            node = currency[0:3]        # from token
            neighbor = currency[3:6]    # to token

            # exchange rate between the currencies
            exchRate = fxp.getExchangeRate(data[14:22])
            # print out the data to console
            print("{}: {} to {}: {}".
                  format(timestamp, node, neighbor, exchRate))

            # struct.error: unpack requires a buffer of 8 bytes
            # reserved = fxp.getReserved(data[22:32])

            # add currency pair to the graph
            self.add_nodes_toGraph(node, neighbor,
                                   exchRate, timestampSeconds)

        else:
            print("--------IGNORING OUT OF SEQUENCE QUOTE!--------")

    def processPath(self, path, startnode):
        """
        process the arbitrage path and print out the order the
        exchanges occurred.

        :param path: Arbitrage path dictionary of currency pairs
            keys are the successor and the value is the parent
            currency.
        :param startnode: Source node/token (USD)
        """
        # print("SOURCE NODE: {}".format(startnode))
        # print("ARBITRAGE PATH PREPROCESS: {}".format(path))
        # KEY = SUCCESSOR
        # VALUE = PARENT

        try:

            START = startnode
            parentNode = path[START]
            # Path = {USD: JPY, JPY:CAD, CAD: GBP, GBP:USD}
            arbitragePath = []

            # visited array to make sure we don't get
            # stuck in an infinite loop. If we find a repeat
            # convert back to USD.
            visited = []

            # Iterate through dictionary going backwards find parent of each
            # token and then appending to the arbitrage path

            while START != parentNode:
                arbitragePath.append(parentNode)
                if parentNode not in visited:
                    visited.append(parentNode)
                else:
                    break
                parentNode = path[parentNode]
            # Loop will provide the backwards order of the exchanges
            # that took place and it will need to be flipped.

            # [USD JPY CAD GBP USD] backwards order

            # reverse the order to canonical below
            # [USD GBP CAD JPY USD] canonical order

            # print("ARBITRAGE PATH: {}".format(arbitragePath))

            # [JPY CAD GBP, USD]
            arbitragePath.append(START)     # add source node to end of list

            # [USD, GBP, CAD, JPY]
            arbitragePath.reverse()         # reverse list to canonical order

            # [USD, GBP, CAD, JPY, USD]
            arbitragePath.append(START)     # add source node to end of list

            # List will now hold its path from source node back to source node
            print("ARBITRAGE PATH: {}".format(arbitragePath))
            self.calculateProfit(arbitragePath)

        except Exception as e:
            print(e)
            print("Arbitrage source node key-error.\n"
                  "Please be patient as the system resets.")
    def calculateProfit(self, arbitrage_path):
        """
        From an initial investment in our Source Node currency we calculate
        the potential profit from following our arbitrage path and print it out
        to the console.
        :param arbitrage_path: in order list representing each token visited
            in the aribtrage path
        :return:
        """
        # based off initial investment calculate and print out

        try:
            accumulated = INITIAL_INVESTMENT
            for i in range(len(arbitrage_path)):

                if i + 1 != len(arbitrage_path):
                    parentToken = arbitrage_path[i]
                    successorToken = arbitrage_path[i + 1]

                    # get conversion rates and convert back to decimal values
                    edge = self.coinbase.graph[parentToken][successorToken][0]
                    rate = 10 ** (-1 * edge)
                    totalPast = accumulated
                    accumulated = accumulated * rate

                    self.printExchange(totalPast, parentToken,
                                       successorToken, rate,
                                       accumulated)
        except Exception as e:
            print("Error occurred when calculating profit")
            print(e)

    def add_nodes_toGraph(self, node, neighbor, exchangeRate, timestamp):
        """
        Adds a new node (token) to the graph and its neighbor, along with
        the edge connecting the two nodes.
        :param node: (String) token
        :param neighbor: (String) another currency that's a neighbor to token
        :param exchangeRate: conversion rate between the two tokens
        :param timestamp: time when the quote was made
        :return:
        """
        self.coinbase.add_node(node)
        self.coinbase.add_edge(node, neighbor, exchangeRate, timestamp)

    @staticmethod
    def printExchange(totalPast, parentToken, successorToken, rate,
                      accumulated):
        """

        :param totalPast:
        :param parentToken:
        :param successorToken:
        :param rate:
        :param accumulated:
        :return:
        """
        print("{} of {} exchanged to {} at rate: {} = {}".format(
            totalPast,
            parentToken, successorToken,
            rate,
            accumulated))

    @staticmethod
    def split(a, n):
        """
        Takes a list of data and splits in into n lists and returns it
        in one list.
        :param a: data to be split
        :param n: the number of chunks you want
        :return: list of n chunks of the original data
        """
        k, m = divmod(len(a), n)
        return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in
                range(n))

    def run(self):
        """
        Runs a loop to join publisher network and receive
        data from the publisher.

        Data is processed and money is made. See above for details.
        """
        print("Lets make some MONEY")
        sub.joinPublisher()
        sub.subscriberListen()
        print("Sending a publisher join request...")


if __name__ == '__main__':
    sub = subscriber('127.0.0.1', 54444)
    sub.run()
