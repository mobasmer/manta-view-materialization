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
        self.overall_scores = []
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

        if v1 < v2:
            i, j = v1, v2 - v1 - 1
        else:
            i, j = v2, v1 - v2 - 1
        return self.pairwise_scores[i][j]

    ''' 
    Computes the similarity for all scores - overall and pairwise - and stores them for later use.
    '''

    def compute_scores(self):
        n = len(self.views)
        for i, view, num_contexts in self.views:
            sum_sim = 0
            for j, other, num_contexts_other in self.views:
                if i == j:
                    sum_sim += 1
                #elif i > j:
                #    sum_sim += self.__get_score__(i, j)
                else:
                    if self.pairwise_score[i][j] != -1:
                        sum_sim += self.pairwise_score[i][j]
                    else:
                        sim = self.similarity_function((view, num_contexts), (other, num_contexts_other))
                        #self.pairwise_scores[i].append(sim)
                        self.pairwise_score[i][j] = sim
                        self.pairwise_score[j][i] = sim
                        sum_sim += sim
            # TODO: should we average similarity scores?
            self.overall_scores.append((i, sum_sim / n))

    def compute_scores_parallel(self):
        n = len(self.views)

        def compute_similarity(i, j):
            view_index, view, num_contexts = self.views[i]
            other_index, other, num_contexts_other = self.views[j]
            sim = self.similarity_function((view, num_contexts), (other, num_contexts_other))
            return sim, view_index, other_index # return similarity score and indices of views

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(compute_similarity, i, j) for i in range(n) for j in range(i + 1, n)]
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures),
                               desc="Computing pairwise similarities"):
                sim, i, j = future.result()
                self.pairwise_score[i][j] = sim
                self.pairwise_score[j][i] = sim


        for i in range(n):
            sum_sim = sum(self.pairwise_score[i][j] for j in range(n) if i != j)
            self.overall_scores.append((i, sum_sim + 1 / n))
    '''
    @param k: number of views to select
    
    @return: list of tuples (view_index, score, max_sim_to_sel, time) of selected views
    '''
    @abstractmethod
    def select_view_indices(self, k):
        pass

