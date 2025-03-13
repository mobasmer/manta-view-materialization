import time
from select import select

from src.util.similarity_measures import jaccard_sim_edges


class SubsetSelector:
    '''
    @param views: list of tuples (index, view) where view is a list of edges
    @param weight: weight for the MMR score (default 0.5)

    Initializes the SubsetSelector with the given views and computes the similarity scores.
    '''

    def __init__(self, views, weight=0.5):
        self.views = views
        self.overall_scores = []
        self.pairwise_scores = [[] for y in range(len(views))]
        self.weight = weight

        self.compute_scores()

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
        Gets next best view to select based on adapted MMR score.
    '''

    def select_next_view(self, sel_indices):
        max_score = -2
        next_view = None
        max_sim_to_prev = None
        if len(sel_indices) == 0:
            # in first step, just choose view with best overall score
            for i, score in self.overall_scores:
                if score > max_score:
                    max_score = score
                    next_view = i
        else:
            # compute MMR score for each view and choose the one with the highest score
            for i, view in self.views:
                if i not in sel_indices:
                    similarities = [self.__get_score__(i, j) for j in sel_indices]
                    max_sim_to_prev = max(similarities)
                    mmr_score = self.weight * self.overall_scores[i][1] - (1 - self.weight) * max_sim_to_prev
                    if mmr_score > max_score:
                        max_score = mmr_score
                        next_view = i
        return next_view, max_score, max_sim_to_prev

    ''' 
    Computes the similarity for all scores - overall and pairwise - and stores them for later use.
    '''

    def compute_scores(self):
        n = len(self.views)
        for i, view in self.views:
            sum_sim = 0
            for j, other in self.views:
                if i == j:
                    sum_sim += 1
                elif i > j:
                    sum_sim += self.__get_score__(i, j)
                else:
                    sim = jaccard_sim_edges(view, other)
                    self.pairwise_scores[i].append(sim)
                    sum_sim += sim
            # TODO: should we average similarity scores?
            self.overall_scores.append((i, sum_sim / n))
        # Rank views by overall score
        # self.overall_scores.sort(key=lambda x: x[1], reverse=True)

    '''
    @param k: number of views to select

    @return: list of tuples (view_index, score, max_sim_to_sel, time) of selected views
    '''

    def select_view_indices(self, k):
        selected_results = []

        if k >= len(self.views):
            raise ValueError("k must be less than the number of views")

        for i in range(k):
            next_view = self.select_next_view([x[0] for x in selected_results])
            selected_results.append(next_view + (time.time(),))

        return selected_results

