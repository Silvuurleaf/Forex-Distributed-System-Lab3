import math
TOLERANCE = 0.0001



class Graph(object):

    def __init__(self):
        self.vertices = []
        self.graph = {}

    def vert_in_graph(self, node):
        return node in self.vertices

    def add_node(self, node):
        if not self.vert_in_graph(node):
            self.vertices.append(node)
            self.graph[node] = {}
        else:
            pass

    def add_edge(self, node, neighbor, weight):
        self.graph[node][neighbor] = -math.log10(weight)

        self.add_node(neighbor)

        print("INVERSE: {}".format(-math.log10(1 / weight)))
        self.graph[neighbor][node] = -math.log10(1 / weight)

        # self.edges.append(Edge(node, neighbor, weight))
        # self.edges.append(Edge(neighbor, node, 1 / weight))

    def remove_node(self, node):
        del self.graph[node]
        # remove reference to node from all vertices

    def destination_predecessors(self, start_node):
        destination = {}  # stores distances from start node
        path = {}  # store path for an arbitrage

        # add currency nodes to destination/predecessor dictionaries
        for node in self.vertices:
            destination[node] = float('inf')
            path[node] = None

        # print("Destination Dictionary: {}".format(destination))

        # know distance from self is zero
        destination[start_node] = 0

        return destination, path

    def bellman_ford(self, startNode):

        # can't run arbitrage if there is only one node in graph
        if len(self.vertices) > 1:

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
                        return self.getNegativeCycle(path, startNode)
        else:
            return None

    def relaxEdge(self, token, neighbor, edge, destination, predecessors):
        # If the distance between the node and the neighbour is lower than the one I have now

        # print("START NODE: {}".format(token))
        # print("RELAX EDGE: {}".format(destination))
        # print("NEIGHBOR: {}".format(neighbor))

        if destination[neighbor] > destination[token] + edge + TOLERANCE:

            #& destination[neighbor] > destination[token] + edge - TOLERANCE:

            # Record this lower distance
            destination[neighbor] = destination[token] + edge

            predecessors[neighbor] = token
            predecessors[token] = neighbor

    def getNegativeCycle(self, path, startNode):

        arbit_path = {startNode: 0}

        arbit_loop = []
        next_node = startNode

        while True:
            prev_node = next_node           # parent node
            next_node = path[next_node]     # successor node

            print("PARENT NODE: {}".format(prev_node))
            print("SUCCESSOR NODE: {}".format(next_node))
            print("PATH: {}".format(path))

            if next_node in arbit_path:
                edge = self.graph[prev_node][next_node]
                print("EDGE: {}".format(edge))
                edge = 10**(-1*edge)
                print("CONVERSION FROM {} to {}: {}".format(prev_node, next_node, edge))
                arbit_loop.append(prev_node)
                arbit_path[prev_node] = edge

                arbit_loop.append(next_node)
                return arbit_loop
            else:
                edge = self.graph[prev_node][next_node]
                edge = 10**(-1*edge)
                print(
                    "CONVERSION FROM {} to {}: {}".format(prev_node, next_node,
                                                          edge))
                arbit_path[prev_node] = edge
                arbit_loop.append(prev_node)



            """
            if next_node not in path:
                arbitrageLoop.append(next_node)
                # add exhange rates to the path
                edge = self.graph[prev_node][next_node]
                arbit_path[prev_node] = edge
            else:
                arbitrageLoop.append(next_node)
                arbitrageLoop = arbitrageLoop[arbitrageLoop.index(next_node):]

                edge = self.graph[prev_node][next_node]
                arbit_path[next_node] = edge

                print("ArbitrageLoop: {}".format(arbitrageLoop))
                return arbit_path
            """
