import networkx as nx
import pandas as pd
from pathlib import Path

from pandas.core.series import Series


class SNAP():
    def __init__(self):
        self.nodes = pd.read_csv('data/nodes.csv')
        self.edges = pd.read_csv('data/edges.csv')
        self.node_array = self.nodes['id_lattes']

    def generate_a_compatible_nodes(self, *atributes):
        attrs = list(atributes)
        a_compatible_nodes = self.nodes.sort_values(attrs)
        self.supernodes = self.nodes.groupby(attrs).groups
        for key in self.supernodes:
            self.supernodes[key] = set(self.supernodes[key])
        print(self.supernodes)
        self.bitmap = pd.DataFrame(
            index=self.nodes['id_lattes'].to_list(), columns=self.supernodes.keys())
        return a_compatible_nodes

    def generate_a_compatible_edges(self, *atributes):
        pass


s = SNAP()
s.generate_a_compatible_nodes('major_area')
