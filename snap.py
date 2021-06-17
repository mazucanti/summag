import networkx as nx
import pandas as pd
from pathlib import Path

from pandas.core.series import Series


class SNAP():
    def __init__(self):
        self.nodes = pd.read_csv('data/nodes.csv')
        self.edges = pd.read_csv('data/edges.csv')
        self.node_array = self.nodes['id_lattes']
        self.neighbor_node_bmp = pd.DataFrame(index=self.node_array)

    def generate_a_compatible(self, *atributes):
        attrs = list(atributes)
        a_compatible_nodes = self.nodes.sort_values(attrs)
        self.groups = self.nodes.groupby(attrs, axis=1)
        print(self.groups.all())
        return a_compatible_nodes


s = SNAP()
s.generate_a_compatible('major_area')
