import math
TOLERANCE = 0.01


class Graph(object):

    def __init__(self):
        self.vertices = []
        self.graph = {}


    def add_node(self, node):
        if node not in self.vertices:
            self.vertices.append(node)
            self.graph[node] = {}
        else:
            pass

    def add_edge(self, node, neighbor, weight):
        self.graph[node][neighbor] = -math.log10(weight)

        self.add_node(neighbor)
        self.graph[neighbor][node] = -math.log10(1/weight)

    def destination_predecessors(self, start_node):
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
        # If the distance between the node and the neighbour is lower than the one I have now
        if (destination[neighbor] > destination[token] + edge + TOLERANCE) &\
                (destination[neighbor] > destination[token] + edge - TOLERANCE):
            # Record this lower distance
            destination[neighbor] = destination[token] + edge

            predecessors[neighbor] = token

