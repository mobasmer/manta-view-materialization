import json
import logging
import pickle

import duckdb
from abc import abstractmethod

import numpy as np
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

results_path = "results/"
class DBSubsetSelector:
    def __init__(self,  db_name, object_types=None, counts_precomputed=False, duckdb_config=None, file_id=None):
        self.db_name = db_name
        self.duckdb_config = duckdb_config
        self.overall_scores = [-1 for _ in range(len(object_types))]
        self.pairwise_score = np.full((len(object_types),len(object_types)), fill_value = -1, dtype = float)
        self.object_types = object_types
        self.counts_precomputed = counts_precomputed
        self.file_id = file_id

        self.compute_scores()
        logging.info("Computed scores")

    '''
     Returns stored pairwise score for v1 and v2.
     '''
    def __get_score__(self, v1, v2):
        return self.pairwise_score[v1][v2]

    """
    Computes the similarity score between two views.
    
        @param i: index of first view in view list 
        @param j: index of second view in view list
        @return: similarity score and indices of views
    """
    def compute_pairwise_scores(self):
        config = {}
        if self.duckdb_config is not None:
            if "memory_limit" in self.duckdb_config:
                config["memory_limit"] = self.duckdb_config["memory_limit"]
            if "threads" in self.duckdb_config:
                config["threads"] = self.duckdb_config["threads"]
        in_memory = True if self.duckdb_config is not None and "in_memory" in self.duckdb_config and self.duckdb_config["in_memory"] else False

        with (duckdb.connect() if in_memory else duckdb.connect(self.db_name, config = config) as con):
                if not self.counts_precomputed:
                    for obj_type in tqdm(self.object_types):
                        con.sql("DROP TABLE IF EXISTS " + obj_type + "Counts")
                        con.sql("CREATE TABLE IF NOT EXISTS "+ obj_type + "Counts" +"(procExec integer, counts integer)")
                        con.sql("INSERT INTO " + obj_type + "Counts" + " SELECT procExec, COUNT(*) as counts FROM "+ obj_type +" GROUP BY procExec ORDER BY procExec ASC")
                        logging.info("Done computing counts for " + obj_type)
                        con.commit()

                logging.info("Done computing counts")
                n = len(self.object_types)
                for i in tqdm(range(n), desc=("Computing pairwise scores")):
                    for j in tqdm(range(i, n)):
                        if i == j:
                            self.pairwise_score[i][i] = 1
                        else:
                            ot1 = self.object_types[i]
                            ot2 = self.object_types[j]

                            # todo what to do with empty tables? what is the semantics?
                            df = con.sql(f'''WITH intersectEdges AS 
                               (SELECT obj1.procExec as o1contexts, obj2.procExec as o2contexts, COUNT(*) as intersectCounts
                                FROM {ot1} obj1, {ot2} obj2
                                WHERE obj1.edge = obj2.edge 
                                GROUP BY obj1.procExec, obj2.procExec)
                            SELECT intersectEdges.o1contexts, intersectEdges.o2contexts, 
                                CASE WHEN obj1Counts.counts > 0 OR obj2Counts.counts > 0 THEN (SELECT intersectEdges.intersectCounts / (obj1Counts.counts + obj2Counts.counts - intersectEdges.intersectCounts)) ELSE 0 END AS sim
                            FROM intersectEdges, {ot1 + "Counts"} obj1Counts, {ot2 + "Counts"} obj2Counts
                            WHERE intersectEdges.o1contexts = obj1Counts.procExec AND intersectEdges.o2contexts = obj2Counts.procExec''').fetchdf()

                            max_sim_per_o1contexts = df.groupby('o1contexts')['sim'].max().reset_index()
                            max_sim_per_o2contexts = df.groupby('o2contexts')['sim'].max().reset_index()

                            #print(max_sim_per_o2contexts)
                            #print(max_sim_per_o1contexts)

                            # Sum the maximum 'sim' values
                            sum_max_sim = max_sim_per_o1contexts['sim'].sum() + max_sim_per_o2contexts['sim'].sum()

                            # need to get num of all process executions in case one does not share any edge with another process execution
                            # (i.e. not participating in the join above)
                            # will be implicitly incl in sum through adding 0, but needs to be accounted for in total number of process executions
                            ot1_numProcExecs = con.sql(f'''SELECT numProcExecs FROM viewmeta WHERE objecttype = '{ot1}';''').fetchone()[0]
                            ot2_numProcExecs = con.sql(
                                f'''SELECT numProcExecs FROM viewmeta WHERE objecttype = '{ot2}';''').fetchone()[0]

                            # Count the number of unique values in 'o1contexts' and 'o2contexts'
                            #num_unique_o1contexts = df['o1contexts'].nunique()
                            #num_unique_o2contexts = df['o2contexts'].nunique()
                            #print(ot1, ot2, num_unique_o1contexts, num_unique_o2contexts)

                            # Calculate the result
                            sim = sum_max_sim / (ot1_numProcExecs + ot2_numProcExecs)

                            self.pairwise_score[i][j] = sim
                            self.pairwise_score[j][i] = sim

    ''' 
    Computes the similarity scores for all views - overall and pairwise - and stores them for later use.
    '''
    def compute_scores(self):
        n = len(self.object_types)

        self.compute_pairwise_scores()
        logging.info("Computed pairwise scores")
        assert np.all(self.pairwise_score >= 0), "Some pairwise scores are negative"

        for i in range(n):
            sum_sim = np.sum(self.pairwise_score[i])
            self.overall_scores[i] = sum_sim / n

        assert sum([0 if o_score != -1 else 1 for o_score in self.overall_scores]) == 0, "Overall scores not computed"
        logging.info("Computed overall scores")

        with open(results_path + self.file_id + "_overall_scores.json", "w") as f:
            obj2score = {self.object_types[i]: self.overall_scores[i] for i in range(n)}
            json.dump(obj2score, f)
        with open(results_path + self.file_id + "_pairwise_scores.json", "w") as f:
            oo2score = [(self.object_types[i], self.object_types[j], self.pairwise_score[i][j]) for j in range(n) for i in range(n)]
            json.dump(oo2score, f)

    '''
    @param k: number of views to select
    
    @return: list of tuples (view_index, score, max_sim_to_sel, time) of selected views
    '''
    @abstractmethod
    def select_view_indices(self, k):
        pass

