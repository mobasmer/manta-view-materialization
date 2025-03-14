import concurrent.futures
import logging
from abc import abstractmethod

import numpy as np
from tqdm import tqdm

from src.util.similarity_measures import jaccard_sim_edges

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class SubsetSelector:
    def __init__(self, views, similarity_function=jaccard_sim_edges, parallel=False):
        self.views = views
        self.overall_scores = [-1 for _ in range(len(views))]
        self.pairwise_score = np.full((len(views),len(views)), fill_value = -1, dtype = float)
        self.similarity_function = similarity_function
        if parallel:
            self.compute_scores_parallel()
        else:
            self.compute_scores()
        logging.info("Computed scores")

    '''
     Returns stored pairwise score for v1 and v2.
     '''
    def __get_score__(self, v1, v2):
        return self.pairwise_score[v1][v2]

        #
    """
    Computes the similarity score between two views.
    
        @param i: index of first view in view list 
        @param j: index of second view in view list
        @return: similarity score and indices of views
    """
    def compute_similarity(self, i, j):
        view_dict = self.views[i]
        other_dict = self.views[j]
        sim = self.similarity_function((view_dict["relation_index"], view_dict["num_proc_exec"]),
                                       (other_dict["relation_index"], other_dict["num_proc_exec"]))
        return sim, view_dict["view_idx"], other_dict["view_idx"]

    ''' 
    Computes the similarity scores for all views - overall and pairwise - and stores them for later use.
    '''
    def compute_scores(self):
        n = len(self.views)

        for i in range(n):
            for j in range(i, n):
                if i == j:
                    self.pairwise_score[i][i] = 1
                else:
                    sim, i_idx, j_idx = self.compute_similarity(i, j)
                    self.pairwise_score[i_idx][j_idx] = sim
                    self.pairwise_score[j_idx][i_idx] = sim

        assert np.all(self.pairwise_score >= 0), "Some pairwise scores are negative"

        for i in range(n):
            sum_sim = np.sum(self.pairwise_score[i])
            self.overall_scores[i] = sum_sim / n

        assert sum([0 if o_score != -1 else 1 for o_score in self.overall_scores]) == 0, "Overall scores not computed"

    def compute_scores_parallel(self):
        n = len(self.views)

        for i in range(n):
            self.pairwise_score[i][i] = 1

    ## TODO: make more space efficient by using immutable, shared memory?
        with concurrent.futures.ProcessPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(self.compute_similarity, i, j) for i in range(n) for j in range(i + 1, n)]
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures),
                               desc="Computing pairwise similarities"):
                sim, i_idx, j_idx = future.result()
                self.pairwise_score[i_idx][j_idx] = sim
                self.pairwise_score[j_idx][i_idx] = sim

        assert np.all(self.pairwise_score >= 0), "Some pairwise scores are negative"

        for i in range(n):
            #sum_sim = sum(self.pairwise_score[i][j] for j in range(n))
            sum_sim = np.sum(self.pairwise_score[i])
            #self.overall_scores.append((i, sum_sim / n))
            # todo should we average?
            self.overall_scores[i] = sum_sim

        assert sum([0 if o_score != -1 else 1 for o_score in self.overall_scores]) == 0, "Overall scores not computed"
    '''
    @param k: number of views to select
    
    @return: list of tuples (view_index, score, max_sim_to_sel, time) of selected views
    '''
    @abstractmethod
    def select_view_indices(self, k):
        pass

