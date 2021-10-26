import math
TOLERANCE = 0.01
"""
Mark Taylor

Bellman Ford algorithm to find shortest path and negative cycles
in our currency pair graph. 

Negative cycles will indicate an inefficient market and arbitrage
opportunity.

"""

class Graph(object):

    def __init__(self):
        """
            Default constructor for Graph class

            Creates a list to store nodes in graph
            Creates a dictionary to act as the graph
        """
        # Create a list of all the nodes (currency) that exist in the graph
        self.vertices = []

        # Graph is a dictionary that stores dictionaries
        # Each key is a currency and its value is a dictionary
        # that stores all of that currencies' neighbors or pairs
        # and their conversion rates.
        self.graph = {}


    def add_node(self, node):
        """
        Adds a node into the graph
        :param node: (String) representing currency
        """

        # Check to see if currency already in graph
        if node not in self.vertices:
            self.vertices.append(node)
            # initialize an empty dictionary for currency
            self.graph[node] = {}
        else:
            pass

    def add_edge(self, node, neighbor, weight):
        """
        Add an edge, which acts as a the conversion rate
        between two currencies. Physically in our graph it
        acts as the distance between two nodes.

        :param node: (String) Currency that the weight is coming from
        :param neighbor: (String) Currency that the weight is directed at
        :param weight: (Double) Exchange rate between two currencies
        """

        # Assign value as a negative logarithm for Bellman Ford
        # to find a negative cycle
        self.graph[node][neighbor] = -math.log10(weight)

        # Add neighbor node to graph
        self.add_node(neighbor)

        # Inverse conversion rate from neighbor going back
        self.graph[neighbor][node] = -math.log10(1/weight)

    def destination_predecessors(self, start_node):
        """

        :param start_node: (String) source node to initialize
            Bellman Ford logarithm
        :return:
            destination: {dictionary} returns a dictionary that stores
                Distances from source node to every other node
                in the graph.
            path: {dictionary} stores the currencies from a negative
                cycle (arbitrage opportunity)
        """
        destination = {}  # stores distances from start node
        path = {}         # store path for an arbitrage

        # add currency nodes to destination/predecessor dictionaries
        for node in self.graph:
            destination[node] = float('inf')
            path[node] = None
        # print("Destination Dictionary: {}".format(destination))

        # know distance from self is zero
        destination[start_node] = 0

        return destination, path

    def bellman_ford(self, startNode):
        """
        Bellman ford algorithm iterates over the graph V-1 times and
        for every currency it goes through its edges and using
        the destination dictionary checks to see if its  relative
        distances need to be updated.

        Then the graph is checked again to see which edges were
        updated and adds them to the aribtrage path.

        :param startNode: (String) source node where the Bellman
            Ford algorithm starts from.
        :return: Dictionary representing path taken for arbitrage
            cycle.
        """

        # get destination & path dictionaries
        destination, path = self.destination_predecessors(startNode)

        number_Vertices = len(self.vertices)

        # iterate V - 1 times for bellman ford
        for i in range(number_Vertices - 1):
            # for each currency (token) in graph
            for token in self.vertices:
                # for given token, check each of its neighbors and its edge
                for neighbor_token, edge in self.graph[token].items():
                    # print("Neighbors to token {}: {}".format(token, self.graph[token]))

                    # relax (update) edge values
                    self.relaxEdge(token, neighbor_token, edge, destination,
                                       path)
            # print("Destinations: {}".format(destination))

        # Second iteration checks if any of the edge values have been updated.
        for token in self.vertices:
            for neighbor_token, edge in self.graph[token].items():
                if destination[neighbor_token] < destination[token] + edge:
                    return path


    def relaxEdge(self, token, neighbor, edge, destination, predecessors):
        """
        Each edge is checked against the destination dictionary which holds the
        distance between each node and every other node in the graph.

        If the new distance to a node is less than what was previously recorded
        update the destination dictionary to reflect the new edge.

        Additionally, path is updated in the predecessor dictionary when an edge is
        relaxed.

        :param token: (string) Current token we are measuring FROM (relative to)
        :param neighbor: (string) currency neighbor to token we are measure
            from.
        :param edge: (double) the edge weight or distance between two nodes
        :param destination: (dictionary) store distances from nodes to its neighbors
        :param predecessors: (dictionary) store path of arbitrage
        :return:
        """
        # If the distance between the node and the neighbour is lower than the one I have now
        if (destination[neighbor] > destination[token] + edge + TOLERANCE) &\
                (destination[neighbor] > destination[token] + edge - TOLERANCE):
            # Record this lower distance
            destination[neighbor] = destination[token] + edge

            predecessors[neighbor] = token

