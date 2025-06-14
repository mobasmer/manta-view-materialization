import time

from src.strategies.db_selection import DBSubsetSelector


class DBRankingSubsetSelector(DBSubsetSelector):

    """
    @param views: list of tuples (index, view) where view is a list of edges
    @param weight: weight for the MMR score (default 0.5)
    @param similarity_function: function to compute similarity between two views (default jaccard_sim_edges)
                    - takes tuple of view representation with number of contexts

    Initializes the SubsetSelector with the given views and computes the similarity scores.
    """
    def __init__(self, db_name, object_types=None, counts_precomputed=False, weight=0.5,
                 duckdb_config=None, file_id=None):
        super().__init__(db_name, object_types, counts_precomputed, duckdb_config, file_id)
        self.weight = weight

    '''
        Gets next best view to select based on adapted MMR score.
    '''

    def select_next_view(self, sel_indices):
        max_score = -2
        next_view = None
        info_scores = {}
        if len(sel_indices) == 0:
            # in first step, just choose view with best overall score
            # for i, score in enumerate(self.overall_scores):
            for i, score in enumerate(self.overall_scores):
                if score > max_score:
                    max_score = score
                    next_view = i
                    info_scores = {
                        "sim_score": score,
                        "mmr_score": score,
                        "max_sim_to_prev": None,
                        "min_sim_to_prev": None,
                        "avg_sim_to_prev": None}
        else:
            # compute MMR score for each view and choose the one with the highest score
            # todo adapt s.t. selected are not integrated in max sim?
            for idx, _ in enumerate(self.object_types):
                if idx not in sel_indices:
                    similarities = [self.__get_score__(idx, j) for j in sel_indices]
                    max_sim_to_prev = max(similarities)
                    sim_score = self.overall_scores[idx]
                    #sim_score = sum([self.__get_score__(idx, j) for j in range(len(self.object_types)) \
                    #                   if j != idx and j not in sel_indices])/n-
                    mmr_score = self.weight * sim_score - (1 - self.weight) * max_sim_to_prev
                    if mmr_score > max_score:
                        max_score = mmr_score
                        next_view = idx
                        info_scores = {
                            "sim_score": self.overall_scores[idx],
                            "mmr_score": mmr_score,
                            "max_sim_to_prev": max(similarities),
                            "min_sim_to_prev": min(similarities),
                            "avg_sim_to_prev": sum(similarities) / len(similarities)
                        }
        return next_view, max_score, info_scores

    '''
    @param k: number of views to select

    @return: list of tuples (view_index, score, max_sim_to_sel, time) of selected views
    '''

    def select_view_indices(self, k):
        selected_results = []

        if k > len(self.object_types):
            raise ValueError("k must be less than the number of views")

        for i in range(k):
            next_view = self.select_next_view([x[0] for x in selected_results])
            selected_results.append(next_view + (time.time(),))

        return selected_results

