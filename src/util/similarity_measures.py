def jaccard_sim_edges(edges1, edges2):
    intersection = edges1.intersection(edges2)
    if len(intersection) == 0:
        return 0
    return len(intersection) / (len(edges1) + len(edges2) - len(intersection))