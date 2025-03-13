import itertools
import time

from src.strategies.selection import SubsetSelector
from src.util.similarity_measures import matching_similarities


class EnumeratingSubsetSelector(SubsetSelector):
    def __init__(self, views, similarity_function=matching_similarities):
        super().__init__(views, similarity_function=similarity_function)

    '''
       @param k: number of views to select

       @return: list of tuples (view_index, score, max_sim_to_sel, time) of selected views
    '''
    def select_view_indices(self, k):
        selected_results = []

        if k > len(self.views):
            raise ValueError("k must be less than the number of views")

        best_view_set = None
        best_score = 0
        for subset in itertools.combinations(self.views, k):
            sum_sim = 0
            for view in self.views:
                nearest_score = max([self.__get_score__(view[0], sel[0]) for sel in subset])
                sum_sim += nearest_score

            if sum_sim > best_score:
                best_score = sum_sim
                best_view_set = subset

        end_time = time.time()
        for sel in best_view_set:
            for sel2 in best_view_set:
                if sel == sel2:
                    continue
                max_sim = max([self.__get_score__(sel[0], sel2[0]) for sel in best_view_set])

            selected_results.append((sel[0], best_score, max_sim, end_time))
        return selected_results