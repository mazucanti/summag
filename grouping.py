import logging
from pathlib import Path
import pandas as pd
import networkx as nx
import datetime as dt


class Grouping():
    def __init__(self, nodes_path, edges_path, sample_size=None, year=None):
        self._setup_logger()
        # self.logger.info('Loading data...')
        self._load_data(nodes_path, edges_path, sample_size, year)

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

    def _load_data(self, nodes_path, edges_path, sample_size, year):
        nodes = pd.read_csv(nodes_path)
        edges = pd.read_csv(edges_path)
        if not year is None:
            edges = edges[edges.conclusion_year == year]
        rename_map = {'target_id_lattes': 'id_lattes'}
        drop_list = ['area', 'major_area']
        nodes = nodes.merge(edges[edges.academic_degree == 'doutorado'].rename(
            columns=rename_map).drop(columns=drop_list), on='id_lattes')
        nodes = nodes.drop(columns='source_id_lattes')
        edges = edges[['source_id_lattes', 'target_id_lattes']]
        if not sample_size is None:
            self.logger.info(f'Generating a sample of size {sample_size}...')
            edges = edges.sample(sample_size)
        source_nodes = edges['source_id_lattes'].rename('id_lattes')
        target_nodes = edges['target_id_lattes'].rename('id_lattes')
        all_nodes = pd.concat([source_nodes, target_nodes]).drop_duplicates()
        nodes = nodes.merge(all_nodes, on='id_lattes')
        self.nodes = nodes
        self.edges = edges

    def generate_flow_graph(self, attribute):
        filter = ['id_lattes', attribute]
        flow = self.edges.merge(
            self.nodes[filter],
            right_on='id_lattes',
            left_on='source_id_lattes')
        flow = flow.merge(
            self.nodes[filter],
            right_on='id_lattes',
            left_on='target_id_lattes'
        )
        flow.columns = [
            col.replace(f'{attribute}_x', 'source')
            for col in flow.columns]
        flow.columns = [col.replace(f'{attribute}_y', 'target')
                        for col in flow.columns]
        flow.to_csv('debug.csv')
        flow = flow.groupby(
            ['source', 'target']
        ).size().reset_index().rename(
            columns={0: 'weight'}
        )
        self.flow = flow
        F = nx.from_pandas_edgelist(
            flow,
            edge_attr='weight',
            create_using=nx.DiGraph
        )
        return F
