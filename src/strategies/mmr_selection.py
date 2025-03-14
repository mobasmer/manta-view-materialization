import time
from select import select

from src.strategies.selection import SubsetSelector
from src.util.similarity_measures import jaccard_sim_edges


class RankingSubsetSelector(SubsetSelector):

    """
    @param views: list of tuples (index, view) where view is a list of edges
    @param weight: weight for the MMR score (default 0.5)
    @param similarity_function: function to compute similarity between two views (default jaccard_sim_edges)
                    - takes tuple of view representation with number of contexts

    Initializes the SubsetSelector with the given views and computes the similarity scores.
    """
    def __init__(self, views, weight=0.5, similarity_function=jaccard_sim_edges):
        super().__init__(views, similarity_function)
        self.weight = weight


    '''
        Gets next best view to select based on adapted MMR score.
    '''

    def select_next_view(self, sel_indices):
        max_score = -2
        next_view = None
        max_sim_to_prev = None
        if len(sel_indices) == 0:
            # in first step, just choose view with best overall score
            # for i, score in enumerate(self.overall_scores):
            for i, score in enumerate(self.overall_scores):
                if score > max_score:
                    max_score = score
                    next_view = i
        else:
            # compute MMR score for each view and choose the one with the highest score
            for i, view, _ in self.views:
                if i not in sel_indices:
                    similarities = [self.__get_score__(i, j) for j in sel_indices]
                    max_sim_to_prev = max(similarities)
                    #mmr_score = self.weight * self.overall_scores[i][1] - (1 - self.weight) * max_sim_to_prev
                    mmr_score = self.weight * self.overall_scores[i] - (1 - self.weight) * max_sim_to_prev
                    if mmr_score > max_score:
                        max_score = mmr_score
                        next_view = i
        return next_view, max_score, max_sim_to_prev

    '''
    @param k: number of views to select

    @return: list of tuples (view_index, score, max_sim_to_sel, time) of selected views
    '''

    def select_view_indices(self, k):
        selected_results = []

        if k > len(self.views):
            raise ValueError("k must be less than the number of views")

        for i in range(k):
            next_view = self.select_next_view([x[0] for x in selected_results])
            selected_results.append(next_view + (time.time(),))

        return selected_results

