from select import select


class SubsetSelector:
    def __init__(self, views, weight=0.5):
        self.views = views
        self.overall_scores = []
        self.pairwise_scores =  [[] for y in range(len(views))]
        self.weight = weight

        self.compute_scores()

    def __get_score__(self, v1, v2):
        # storing only upper triangular matrix
        # e.g. 8 > 7, then we computed similarity score for 7 with 8
        # -> have to use index s.t. view 8 is at index 0

        if v1 < v2:
            i, j = v1, v2-v1-1
        else:
            i, j = v2, v1-v2-1
        return self.pairwise_scores[i][j]

    def select_next_view(self, sel_indices):
        if len(sel_indices) == 0:
            return self.overall_scores[0]
        else:
            next_view = None
            max_score = -2
            for i, view in self.views:
                if i not in sel_indices:
                    similarities = [self.__get_score__(i, j) for j in sel_indices]
                    mmr_score = self.weight * self.overall_scores[i][1] - (1-self.weight) * max(similarities)
                    if mmr_score > max_score:
                        max_score = mmr_score
                        next_view = i
            return next_view, max_score

    def compute_scores(self):
        # TODO: should we average similarity scores?
        n = len(self.views)
        for i, view in self.views:
            sum_sim = 0
            for j, other in self.views:
                if i == j:
                    sum_sim += 1
                elif i > j:
                    sum_sim += self.__get_score__(i,j)
                else:
                    sim = self.jaccard_sim_edges(view, other)
                    self.pairwise_scores[i].append(sim)
                    sum_sim += sim
            self.overall_scores.append((i, sum_sim/n))
        self.overall_scores.sort(key=lambda x: x[1], reverse=True)

    def jaccard_sim_edges(self, edges1, edges2):
        intersection = edges1.intersection(edges2)
        if len(intersection) == 0:
            return 0
        return len(intersection) / (len(edges1) + len(edges2) - len(intersection))

    # have to pass edges as argument
    def select_view_indices(self, k):
        selected_indices = []
        selected_results = []

        if k >= len(self.views):
            raise ValueError("k must be less than the number of views")

        for i in range(k):
            next_view = self.select_next_view(selected_indices)
            selected_indices.append(next_view[0])
            selected_results.append(next_view)

        return selected_results

