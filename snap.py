import networkx as nx
import pandas as pd
from pathlib import Path

from pandas.core.series import Series


class SNAP():
    def __init__(self):
        self.nodes = pd.read_csv('data/nodes.csv', index_col=0)
        self.edges = pd.read_csv('data/edges.csv')
        self.node_array = self.nodes.index

    def generate_a_compatible_nodes(self, *atributes):
        attrs = list(atributes)
        a_compatible_nodes = self.nodes.sort_values(attrs)
        self.supernodes = self.nodes.groupby(attrs).groups
        self._initialize_bitmap()
        
        return a_compatible_nodes

    def _initialize_bitmap(self):
        self.bitmap = pd.DataFrame(
            index=self.nodes.index.to_list(), columns=self.supernodes.keys())
        for source in self.supernodes.keys():
            self.bitmap[source] = 0
            for target in self.supernodes.keys():
                source_nodes = pd.Series(self.supernodes[source])
                target_nodes = pd.Series(self.supernodes[target])
                edges = self.edges.merge(
                    source_nodes, left_on='source_id_lattes', right_on='id_lattes', how='inner')
                edges = edges.merge(
                    target_nodes, left_on='target_id_lattes', right_on='id_lattes', how='inner')
                neighbour_nodes = edges['source_id_lattes'].to_list()
                self.bitmap.loc[neighbour_nodes, source] = 1
    


nodes = pd.read_csv('data/nodes.csv', index_col=0)
s = SNAP()
s.generate_a_compatible_nodes('major_area')
