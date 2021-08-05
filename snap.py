import pandas as pd
import networkx as nx
from itertools import cycle


class SNAP():
    def __init__(self, nodes_path, edges_path):
        self.nodes = pd.read_csv(nodes_path, index_col=0)
        self.edges = pd.read_csv(edges_path)

    def generate_a_compatible_nodes(self, *attributes):
        attrs = list(attributes)
        self.supernodes = self.nodes.groupby(attrs).groups
        self.bitmap = pd.DataFrame(0,
                                   index=self.nodes.index.to_list(),
                                   columns=self.supernodes.keys())
        for supernode, nodes in self.supernodes.items():
            self.update_bitmap(supernode, *nodes)

    def update_bitmap(self, supernode, *nodes):
        self.bitmap[supernode] = 0
        neighbours = self.edges['target_id_lattes'].isin(nodes)
        neighbours = self.edges[neighbours]['source_id_lattes']
        try:
            self.bitmap.loc[neighbours, supernode] = 1
        except KeyError:
            pass

    def generate_ar_compatible_nodes(self, *attributes):
        self.generate_a_compatible_nodes(*attributes)
        while True:
            size = len(self.supernodes)
            supernodes = self.supernodes.copy()
            for supernode, nodes in supernodes.items():
                participation_array = self.bitmap.loc[nodes, :].sum()
                if participation_array.isin([0, len(nodes)]).all():
                    continue
                new_supernodes = self.generate_new_supernodes(nodes)
                self.update_supernodes(new_supernodes, supernode)
            if size == len(self.supernodes):
                break

    def update_supernodes(self, new_supernodes, supernode):
        new_nodes = new_supernodes.popitem()[1]
        self.supernodes[supernode] = new_nodes
        self.update_bitmap(supernode, *new_nodes)
        i = 0
        for new_supernode, new_nodes in new_supernodes.items():
            supernode_name = f'{supernode}_{i}'
            i += 1
            self.supernodes[supernode_name] = new_nodes
            self.update_bitmap(supernode_name, *new_nodes)

    def generate_new_supernodes(self, nodes):
        cols = self.bitmap.columns.to_list()
        new_supernodes = self.bitmap.loc[nodes, :]
        new_supernodes = new_supernodes.groupby(cols).groups
        return new_supernodes

    def generate_graph(self, file_name):
        G = nx.DiGraph()
        for supernode, nodes in self.supernodes.items():
            neighbours = list(self.supernodes.keys())
            nodes_adjency = self.bitmap.loc[nodes, :]
            weights = nodes_adjency.sum()[neighbours].to_list()
            for i, weight in enumerate(weights):
                if weight != 0:
                    G.add_edge(supernode, neighbours[i], weight=weight)
        nx.write_graphml(G, file_name)


if __name__ == '__main__':

    s = SNAP('data/nodes.csv', 'data/edges.csv')
    s.generate_ar_compatible_nodes('major_area')
    s.generate_graph('ar_comp.graphml')
