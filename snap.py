import pandas as pd
import networkx as nx
from itertools import cycle


class SNAP():
    __slots__ = ['nodes','edges','bitmap','supernodes']
    def __init__(self, nodes_path, edges_path):
        print('Loading data...')
        self.nodes = pd.read_csv(nodes_path, index_col=0)
        self.edges = pd.read_csv(edges_path, usecols=['source_id_lattes', 'target_id_lattes'])

    def generate_a_compatible_nodes(self, *attributes):
        attrs = list(attributes)
        self.nodes = self.nodes[attrs]
        self.supernodes = self.nodes.groupby(attrs).groups
        self.bitmap = pd.DataFrame(0,
                                   index=self.nodes.index.to_list(),
                                   columns=self.supernodes.keys())
        print('Initializing bitmap...')
        for supernode, nodes in self.supernodes.items():
            self.update_bitmap(supernode, *nodes)

    def update_bitmap(self, supernode, *nodes):
        neighbours = self.edges['target_id_lattes'].isin(nodes)
        neighbours = self.edges[neighbours]['source_id_lattes'].drop_duplicates()
        cols = set(self.bitmap.columns.to_list())
        if supernode in cols:
            self.bitmap.loc[neighbours, supernode] = 1
        else:
            new_nodes = pd.Series(1,index=neighbours, name=supernode)
            self.bitmap = pd.concat([self.bitmap, new_nodes], axis=1).fillna(0)
        

    def generate_ar_compatible_nodes(self, *attributes):
        self.generate_a_compatible_nodes(*attributes)
        while True:
            print('Generating AR compatible nodes...')
            size = len(self.supernodes)
            supernodes = self.supernodes.copy()
            for supernode, nodes in supernodes.items():
                print(f'Splitting {supernode}...')
                participation_array = self.bitmap.loc[nodes, :].sum()
                if participation_array.isin([0, len(nodes)]).all():
                    continue
                print('Generating new groups...')
                new_supernodes = self.generate_new_supernodes(nodes)
                self.update_supernodes(new_supernodes, supernode)
            if size == len(self.supernodes):
                break

    def update_supernodes(self, new_supernodes, supernode):
        new_nodes = new_supernodes.popitem()[1]
        self.supernodes[supernode] = new_nodes
        self.update_bitmap(supernode, *new_nodes)
        i = 0
        print('Inserting new supernodes in the bitmap...')
        for new_supernode, new_nodes in new_supernodes.items():
            supernode_name = f'{supernode}_{i}'
            print(f'Inserting {supernode_name}')
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
        neighbours = list(self.supernodes.keys())
        print('Generating graph...')
        for supernode, nodes in self.supernodes.items():
            nodes_adjency = self.bitmap.loc[nodes, :].copy()
            weights = nodes_adjency.sum()[neighbours].to_list()
            for i, weight in enumerate(weights):
                if weight != 0:
                    G.add_edge(supernode, neighbours[i], weight=weight)
        nx.write_graphml(G, file_name)


if __name__ == '__main__':

    s = SNAP('data/nodes.csv', 'data/edges.csv')
    s.generate_ar_compatible_nodes('major_area')
    s.generate_graph('ar_comp_ma.graphml')
