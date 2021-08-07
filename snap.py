from math import log
import pandas as pd
import networkx as nx
from itertools import cycle
import dask.dataframe as dd
import logging
from pathlib import Path
import datetime as dt


class SNAP():
    __slots__ = ['logger', 'nodes', 'edges',
                 'sample_size', 'supernodes', 'bitmap']

    def __init__(self, nodes_path, edges_path, sample_size=None):
        self._setup_logger()
        self.logger.info('Loading data...')
        self.nodes = pd.read_csv(nodes_path, index_col=0)
        self.edges = pd.read_csv(edges_path, usecols=[
                                 'source_id_lattes', 'target_id_lattes'])
        if not sample_size is None:
            self.logger.info(f'Generating a sample of size {sample_size}...')
            self.nodes, self.edges = self.generate_sample(sample_size)

    def _setup_logger(self):
        log_dir = Path('logs/')
        log_name = f'exec_{dt.datetime.today()}.log'
        if not log_dir.is_dir():
            log_dir.mkdir(exist_ok=True, parents=True)
        log_path = log_dir / log_name

        fmt = logging.Formatter('(%(funcName)s)-%(levelname)s: %(message)s')
        sh = logging.StreamHandler().setFormatter(fmt)
        fh = logging.FileHandler(log_path).setFormatter(fmt)

        self.logger = logging.getLogger('SNAP')
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(sh)
        self.logger.addHandler(fh)

    def generate_sample(self, sample_size):
        edges = self.edges.sample(sample_size)
        source_nodes = edges['source_id_lattes']
        target_nodes = edges['target_id_lattes']
        nodes = pd.concat([self.nodes.loc[source_nodes, :],
                          self.nodes.loc[target_nodes, :]])
        nodes.drop_duplicates(inplace=True)
        return edges, nodes

    def generate_a_compatible_nodes(self, *attributes):
        attrs = list(attributes)
        self.nodes = self.nodes[attrs]
        self.supernodes = self.nodes.groupby(attrs).groups
        self.bitmap = pd.DataFrame(0,
                                   index=self.nodes.index.to_list(),
                                   columns=self.supernodes.keys())
        self.bitmap = dd.from_pandas(self.bitmap, 100)
        self.logger.info('Initializing bitmap...')
        for supernode, nodes in self.supernodes.items():
            self._update_bitmap(supernode, *nodes)

    def _update_bitmap(self, supernode, *nodes):
        neighbours = self.edges['target_id_lattes'].isin(nodes)
        neighbours = self.edges[neighbours]['source_id_lattes'].drop_duplicates(
        )
        cols = set(self.bitmap.compute().columns.to_list())
        if supernode in cols:
            bitmap = self.bitmap.compute()[supernode]
            bitmap.loc[neighbours] = 1
            self.bitmap[supernode] = bitmap
        else:
            s = pd.Series(1, index=neighbours, name=supernode)
            self.bitmap.assign(**{supernode: s})

    def generate_ar_compatible_nodes(self, *attributes):
        self.generate_a_compatible_nodes(*attributes)
        while True:
            self.logger.info('Generating AR compatible nodes...')
            size = len(self.supernodes)
            supernodes = self.supernodes.copy()
            for supernode, nodes in supernodes.items():
                self.logger.info(f'Splitting {supernode}...')
                participation_array = self.bitmap.compute().loc[nodes, :].sum()
                if participation_array.isin([0, len(nodes)]).all():
                    continue
                self.logger.info('Generating new groups...')
                new_supernodes = self._generate_new_supernodes(nodes)
                self._update_supernodes(new_supernodes, supernode)
            if size == len(self.supernodes):
                break

    def _update_supernodes(self, new_supernodes, supernode):
        new_nodes = new_supernodes.popitem()[1]
        self.supernodes[supernode] = new_nodes
        self._update_bitmap(supernode, *new_nodes)
        i = 0
        self.logger.info('Inserting new supernodes in the bitmap...')
        for new_supernode, new_nodes in new_supernodes.items():
            supernode_name = f'{supernode}_{i}'
            self.logger.info(f'Inserting {supernode_name}')
            i += 1
            self.supernodes[supernode_name] = new_nodes
            self._update_bitmap(supernode_name, *new_nodes)

    def _generate_new_supernodes(self, nodes):
        cols = self.bitmap.columns.to_list()
        new_supernodes = self.bitmap.compute().loc[nodes, :]
        new_supernodes = new_supernodes.groupby(cols).groups
        return new_supernodes

    def generate_graph(self, file_name):
        G = nx.DiGraph()
        neighbours = list(self.supernodes.keys())
        self.logger.info('Generating graph...')
        self.bitmap = self.bitmap.compute()
        for supernode, nodes in self.supernodes.items():
            nodes_adjency = self.bitmap.loc[nodes, :].copy()
            weights = nodes_adjency.sum()[neighbours].to_list()
            for i, weight in enumerate(weights):
                if weight != 0:
                    G.add_edge(supernode, neighbours[i], weight=weight)
        nx.write_graphml(G, file_name)


if __name__ == '__main__':

    s = SNAP('raw/public_db_vertices.csv',
             'raw/public_db_edges.csv', sample_size=100000)
    s.generate_ar_compatible_nodes('major_area')
    s.generate_graph('data/ar_comp_ma.graphml')
