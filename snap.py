from math import log
import pandas as pd
import networkx as nx
from itertools import cycle
import dask.dataframe as dd
import logging
from pathlib import Path
import datetime as dt


class SNAP():
    __slots__ = ['logger', 'nodes', 'edges', 'supernodes', 'bitmap']

    def __init__(self, nodes_path, edges_path, sample_size=None):
        self._setup_logger()
        self.logger.info('Loading data...')
        self._load_data(nodes_path, edges_path, sample_size)

    def _debug(self, message):
        self.logger.debug(f'\n{message}')

    def _setup_logger(self):
        log_dir = Path('logs/')
        log_name = f'exec_{dt.datetime.today()}.log'
        log_dir.mkdir(exist_ok=True, parents=True)
        log_path = log_dir / log_name

        fmt = logging.Formatter('(%(funcName)s) %(levelname)s: %(message)s')
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        fh = logging.FileHandler(log_path)
        fh.setFormatter(fmt)

        self.logger = logging.getLogger('SNAP')
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(sh)
        self.logger.addHandler(fh)

    def _load_data(self, nodes_path, edges_path, sample_size):
        nodes = pd.read_csv(nodes_path)
        edges = pd.read_csv(edges_path, usecols=[
            'source_id_lattes', 'target_id_lattes'])
        if not sample_size is None:
            self.logger.info(f'Generating a sample of size {sample_size}...')
            edges = edges.sample(sample_size)
        source_nodes = edges['source_id_lattes'].rename('id_lattes')
        target_nodes = edges['target_id_lattes'].rename('id_lattes')
        all_nodes = pd.concat([source_nodes, target_nodes]).drop_duplicates()
        nodes = nodes.merge(all_nodes, on='id_lattes')
        nodes.set_index('id_lattes', inplace=True)
        self.nodes = nodes
        self.edges = edges

    def generate_a_compatible_nodes(self, *attributes):
        attrs = list(attributes)
        self.nodes = self.nodes[attrs]
        self.supernodes = self.nodes.groupby(attrs).groups
        self.logger.info('Initializing bitmap...')
        self._initialize_bitmap()
        for supernode, nodes in self.supernodes.items():
            self._update_bitmap(supernode, *nodes)

    def _initialize_bitmap(self):
        self.bitmap = pd.DataFrame(0, index=self.nodes.index.to_list(),
                                   columns=self.supernodes.keys())
        self.bitmap.reset_index(inplace=True)
        self.bitmap = dd.from_pandas(self.bitmap, 200)

    def _update_bitmap(self, supernode, *nodes):
        neighbours = self.edges['target_id_lattes'].isin(nodes)
        neighbours = self.edges[neighbours][
            'source_id_lattes'].drop_duplicates()
        neighbours = pd.Series(1, index=neighbours, name='id_lattes')
        cols = set(self.bitmap.set_index('index').compute().columns.to_list())
        if supernode in cols:
            bits = self.bitmap.set_index('index').compute()[supernode]
            bits.update(neighbours)
        else:
            bits = neighbours
        self._debug(self.bitmap)
        self.bitmap = self.bitmap.set_index('index').assign(**{supernode: bits}).fillna(0)
        self._debug(self.bitmap)
        self.bitmap.reset_index()

    def generate_ar_compatible_nodes(self, *attributes):
        self.generate_a_compatible_nodes(*attributes)
        while True:
            self.logger.info('Generating AR compatible nodes...')
            size = len(self.supernodes)
            supernodes = self.supernodes.copy()
            for supernode, nodes in supernodes.items():
                self.logger.info(f'Splitting {supernode}...')
                participation_array = self.bitmap.set_index('index').compute().loc[nodes, :].sum()
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
            self.supernodes[supernode_name] = new_nodes
            self._update_bitmap(supernode_name, *new_nodes)
            i += 1

    def _generate_new_supernodes(self, nodes):
        cols = self.bitmap.set_index('index').columns.to_list()
        new_supernodes = self.bitmap.set_index('index').compute().loc[nodes, :]
        new_supernodes = new_supernodes.groupby(cols).groups
        return new_supernodes

    def generate_graph(self, file_name):
        G = nx.DiGraph()
        neighbours = list(self.supernodes.keys())
        self.logger.info('Generating graph...')
        self.bitmap = self.bitmap.set_index('index').compute()
        for supernode, nodes in self.supernodes.items():
            nodes_adjency = self.bitmap.set_index('index').loc[nodes, :].copy()
            weights = nodes_adjency.sum()[neighbours].to_list()
            for i, weight in enumerate(weights):
                if weight != 0:
                    G.add_edge(supernode, neighbours[i], weight=weight)
        nx.write_graphml(G, file_name)


if __name__ == '__main__':

    s = SNAP('data/nodes.csv', 'data/edges.csv', sample_size=100000)
    s.generate_ar_compatible_nodes('major_area')
    s.generate_graph('data/ar_comp_ma.graphml')
