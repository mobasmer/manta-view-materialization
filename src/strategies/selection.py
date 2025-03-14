import concurrent.futures
import logging
from abc import abstractmethod

import numpy as np
from tqdm import tqdm

from src.util.similarity_measures import jaccard_sim_edges

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class SubsetSelector:
    def __init__(self, views, similarity_function=jaccard_sim_edges):
        self.views = views
        #self.overall_scores = []
        self.overall_scores = [-1 for _ in range(len(views))]
        #self.pairwise_scores = [[] for _ in range(len(views))]
        self.pairwise_score = np.full((len(views),len(views)), fill_value = -1, dtype = float)
        self.similarity_function = similarity_function
        self.compute_scores_parallel()
        logging.info("Computed scores")

    '''
     Returns stored pairwise score for v1 and v2.
     '''

    def __get_score__(self, v1, v2):
        # Pairwise scores are stored as only upper triangular matrix only.
        # E.g. with v1=8 and v2=7, 8 > 7,
        # then we computed similarity score for 7 with 8
        # and find similarity score at index 7, 0

        #if v1 < v2:
        #    i, j = v1, v2 - v1 - 1
        #else:
        #    i, j = v2, v1 - v2 - 1
        return self.pairwise_score[v1][v2]

    ''' 
    Computes the similarity for all scores - overall and pairwise - and stores them for later use.
    '''

    def compute_scores(self):
        n = len(self.views)
        for i, view_tuple in enumerate(self.views):
            i_idx, relation_index, num_contexts = view_tuple
            sum_sim = 0
            for j in range(i + 1, n):
                j_idx, relation_index_other, num_contexts_other = self.views[j]
                sim = self.similarity_function((relation_index, num_contexts), (relation_index_other, num_contexts_other))
                #self.pairwise_scores[i].append(sim)
                self.pairwise_score[i_idx][j_idx] = sim
                self.pairwise_score[j_idx][i_idx] = sim
                sum_sim += sim
            # TODO: should we average similarity scores?
            self.overall_scores[i_idx] = sum_sim / n
            #self.overall_scores.append((i, sum_sim / n))

    def compute_scores_parallel(self):
        n = len(self.views)

        for i in range(n):
            self.pairwise_score[i][i] = 1

        def compute_similarity(i, j):
            view_index, view, num_contexts = self.views[i]
            other_index, other, num_contexts_other = self.views[j]
            sim = self.similarity_function((view, num_contexts), (other, num_contexts_other))
            return sim, view_index, other_index # return similarity score and indices of views

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(compute_similarity, i, j) for i in range(n) for j in range(i + 1, n)]
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
            self.overall_scores[i] = sum_sim / n

        assert sum([0 if o_score != -1 else 1 for o_score in self.overall_scores]) == 0, "Overall scores not computed"
    '''
    @param k: number of views to select
    
    @return: list of tuples (view_index, score, max_sim_to_sel, time) of selected views
    '''
    @abstractmethod
    def select_view_indices(self, k):
        pass

