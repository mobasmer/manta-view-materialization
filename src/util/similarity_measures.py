import numpy as np


def jaccard_sim_edges(view_info, other_view_info):
    edge_indices = view_info[0]
    other_edge_indices = other_view_info[0]

    edges1 = set(edge_indices.keys())
    edges2 = set(other_edge_indices.keys())
    intersection = edges1.intersection(edges2)
    if len(intersection) == 0:
        return 0
    return len(intersection) / (len(edges1) + len(edges2) - len(intersection))

def matching_similarities(view_info, other_view_info):
    view, num_view = view_info  # view is a dictionary of edges to contexts they are in, num_view is the number of contexts
    other_view, num_other_view = other_view_info # same for other view

    context_edge_counts_view = np.zeros(num_view) # number of edges in each context
    context_edge_counts_other_view = np.zeros(num_other_view) # same for other view
    intersect_counts = np.zeros((num_view, num_other_view)) # rows denote contexts from view,
                                                        # columns denote contexts from other_view,
                                                        # cell (i,j) denotes number of edges in context i that are also in context j

    for edge, contexts in view.items():
        context_edge_counts_view[contexts] += 1
        #for context in contexts:
            #context_edge_counts_view[context] += 1 # count edge for each context it is in in view

    for edge, contexts in other_view.items():
        context_edge_counts_other_view[contexts] += 1
        #for context in contexts:
        #    context_edge_counts_other_view[context] += 1  # count edge for each containing context if has not been counted yet

    if len(view) > len(other_view):
        smaller_view = other_view
        larger_view = view
    else:
        smaller_view = view
        larger_view = other_view

    for edge, contexts in smaller_view.items():
        if edge in larger_view:
            other_contexts = larger_view[edge]

            for context in contexts:
                for other_context in other_contexts:
                    intersect_counts[context, other_context] += 1 # count edge for each pair of contexts it is in in view and other view

    #for edge, contexts in other_view.items():
    #    if edge not in view:
    #        for context in contexts:
    #            context_edge_counts_other_view[context] += 1    # count edge for each containing context if has not been counted yet

    sim_values = np.zeros((num_view, num_other_view))
    #for i in range(num_view):
    #    for j in range(num_other_view):
    #        sim_values[i, j] = intersect_counts[i, j] / (context_edge_counts_view[i] + context_edge_counts_other_view[j] - intersect_counts[i, j])
    sim_values = intersect_counts / (
                context_edge_counts_view[:, None] + context_edge_counts_other_view - intersect_counts)
    #sum_sim = 0
    #for i in range(num_view):
    #    sum_sim += np.max(sim_values[i, :])
    #for j in range(num_other_view):
    #    sum_sim += np.max(sim_values[:, j])

    sum_sim = np.sum(np.max(sim_values, axis=1)) + np.sum(np.max(sim_values, axis=0))

    return sum_sim / (num_view + num_other_view)

def compute_matching_sim(view_dict, other_dict):
    sim = matching_similarities((view_dict["relation_index"], view_dict["num_proc_exec"]),
                             (other_dict["relation_index"], other_dict["num_proc_exec"]))

    return sim, view_dict["view_idx"], other_dict["view_idx"]
