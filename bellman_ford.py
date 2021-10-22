class Graph(object):

    def __init__(self):
        self.vertices = set()
        self.graph = {}

    def vert_in_graph(self, node):
        return node in self.vertices

    def add_node(self, node):
        if not self.vert_in_graph(node):
            self.vertices.add(node)
            self.graph[node] = {}
        else:
            pass

    def add_edge(self, node, neighbor, weight):
        self.graph[node][neighbor] = weight
        self.graph[neighbor][node] = 1/weight

    def remove_node(self, node):
        del self.graph[node]
        # remove reference to node from all vertices

    def toStr(self):
        print(self.graph)

    def bellman_ford(self):
        pass
    """
    def add_edge(self, a, b, c):

        self.graph.append([a, b, c])

    # Print the solution

    def print_solution(self, distance):

        print("Vertex Distance from Source")

        for k in range(self.M):
            print("{0}\t\t{1}".format(k, distance[k]))

    def bellman_ford(self, src):

        distance = [float("Inf")] * self.M
        distance[src] = 0

        for _ in range(self.M - 1):
            for a, b, c in self.graph:
                if distance[a] != float("Inf") and distance[a] + c < distance[b]:
                    distance[b] = distance[a] + c

        for a, b, c in self.graph:
            if distance[a] != float("Inf") and distance[a] + c < distance[b]:
                print("Graph contains negative weight cycle")

                return

        self.print_solution(distance)
    """
