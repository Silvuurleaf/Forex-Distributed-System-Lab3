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
        self.graph[node][neighbor] = weight
        self.add_node(neighbor)
        self.graph[neighbor][node] = 1 / weight

        #self.edges.append(Edge(node, neighbor, weight))
        #self.edges.append(Edge(neighbor, node, 1 / weight))


    def remove_node(self, node):
        del self.graph[node]
        # remove reference to node from all vertices

    def destination_predecessors(self, start_node):
        destination = {}
        predecessors = {}

        #for node, _ in self.graph.items():
        for node in self.vertices:
            destination[node] = float('inf')
            predecessors[node] = None

        # know distance from self is zero
        print("Destination Dictionary: {}".format(destination))
        print("WTF IS START NODE: {}".format(start_node))
        destination[start_node] = 0

        return destination, predecessors

    def bellman_ford(self, startNode):
        if len(self.vertices) > 1:

            destination, pred = self.destination_predecessors(startNode)
            number_Vertices = len(self.vertices)

            for i in range(number_Vertices-1):
                for token in self.vertices:
                    for neighbor_token, edge in self.graph[token].items():
                        print("Neighbors to token {}: {}".format(token, self.graph[token]))
                        self.relaxEdge(token, neighbor_token, edge, destination, pred)


            print("Destinations: {}".format(destination))

            for token in self.vertices:
                for neighbor_token, edge in self.graph[token].items():
                    if destination[neighbor_token] < destination[token] + edge:
                        return self.getNegativeCycle(pred, startNode)
        else:
            return None

    def relaxEdge(self, token, neighbor, edge, destination, predecessors):
        # If the distance between the node and the neighbour is lower than the one I have now

        print("START NODE: {}".format(token))
        print("RELAX EDGE: {}".format(destination))
        print("NEIGHBOR: {}".format(neighbor))

        if destination[neighbor] > destination[token] + edge:
            # Record this lower distance
            destination[neighbor] = destination[token] + edge
            predecessors[neighbor] = token
    def getNegativeCycle(self, predecessors, startNode):
        arbitrageLoop = [startNode]
        print("THE MFKING PREDECESORS {}".format(predecessors))

        return None
        """
                while True:
            next_node = predecessors[startNode]
            if next_node not in arbitrageLoop:
                arbitrageLoop.append(next_node)
            else:
                arbitrageLoop.append(next_node)
                arbitrageLoop = arbitrageLoop[arbitrageLoop.index(next_node):]
                return arbitrageLoop        
        """





class Edge(object):
    def __init__(self, start, destination, weight):
        self.fromNode = start
        self.toNode = destination
        self.edgeWeight = weight
    def __str__(self):
        print("Start: {}, Destination: {}, Weight {}"
              .format(self.fromNode,self.toNode,self.edgeWeight))

        """
        distance ={}
        for currency in self.vertices:
            # dictionary of size V, track best distance to each node from start node
            distance = {currency: 0}

            for currencyPair, edge in self.graph[currency].items():
                # Set distance for currency to each currency pair to infinity
                distance[currencyPair] = float('inf')
                # distance to self is 0, we are starting from some currency


            print("DISTANCES from {} : {}".format(currency, distance))

            for currencyPair, edge in self.graph[currency].items():
                # Relax each edge
                # Check the current distance + edge cost and compare it to the
                # current edge cost from starting node
                if distance[currency] + edge < distance[currencyPair]:
                    # if edge cost to node is less than what was saved previously
                    # update the value.
                    distance[currencyPair] = distance[currency] + edge

            print("AFTER EDGES RELAXED")
            print("DISTANCES from {} : {}".format(currency, distance))

        for currency in self.vertices:

            # DETECT NEGATIVE CYCLE
            for currencyPair, edge in self.graph[currency].items():
                # Relax each edge
                # Check the current distance + edge cost and compare it to the
                # current edge cost from starting node
                print("CURRENT DISTANCE: {}".format(distance[currencyPair]))
                print("NEW DISTANCE: {}".format(edge))
                if distance[currency] + edge < distance[currencyPair]:
                    # if edge cost to node is less than what was saved previously
                    # update the value.
                    distance[currencyPair] = distance[currency] + edge
                    print("ARBITRAGE DETECTED")
        """