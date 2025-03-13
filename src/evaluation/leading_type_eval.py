import datetime
import json
import time
import logging

from src.strategies.enumerating_selection import EnumeratingSubsetSelector
from src.strategies.mmr_selection import RankingSubsetSelector
from src.util.similarity_measures import matching_similarities
from src.view_generation.leading_type_views import load_ocel_by_leading_type, compute_indices_by_leading_type, \
    compute_indices_by_leading_type_parallel

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def main():
    #compute_views_for_bpi17_csv()
    #compute_views_for_bpi14()
    compute_views_for_order_management()
    #compute_views_for_order_management(sequence=True)
    #compute_views_for_order_management()


# TODO: check that event ids are taken from the event log / assigned deterministically
def compute_views(filename, object_types, short_name, file_type="json", params=None, k=2, weight=0.5, sequence=False, selection_method="mmr"):
    start_time = time.time()

    if file_type == "csv":
        indices_leading_types = compute_indices_by_leading_type(filename, file_type=file_type, object_types=object_types, act_name=params["act_name"], time_name=params["time_name"], sep=params["sep"])
    else:
        indices_leading_types = compute_indices_by_leading_type_parallel(filename, file_type=file_type, object_types=object_types)

    logging.info("Done computing indices by leading type")

    if selection_method == "mmr":
        logging.info("Selecting views by mmr")
        ranking_subset_selection = RankingSubsetSelector(views=indices_leading_types, weight=weight, similarity_function=matching_similarities)
        selected_views = ranking_subset_selection.select_view_indices(k)
    else:
        logging.info("Selecting views by enumeration")
        enumerate_subset_selection = EnumeratingSubsetSelector(views=indices_leading_types, similarity_function=matching_similarities)
        selected_views = enumerate_subset_selection.select_view_indices_in_parallel(k)

    print(selected_views)
    logging.info("Computing stats for evaluation")
    get_stats_for_views(filename, selected_views, object_types, indices_leading_types, start_time, f"{selection_method}-leading-type",
                        short_name, file_type=file_type)

    print(selected_views)

def compute_views_for_bpi17(k=4, weight=0.5, sequence=False, selection_method="mmr"):
    filename = 'data/BPIC17.jsonocel'
    object_types = [
        "Application",
        "Workflow",
        "Offer",
        "Case_R"
    ]
    compute_views(filename, object_types, "BPI17", k=k, weight=weight, sequence=sequence)


def compute_views_for_bpi17_csv(k=4, weight=0.5, sequence=False, selection_method="mmr"):
    filename = 'data/BPI2017-Final-adapt.csv'
    object_types = ["offer", "application", "event_org:resource", "event_EventID"]

    parameters = {
        "act_name": 'event_activity',
        "time_name": 'event_timestamp',
        "sep": ',',
    }

    compute_views(filename, object_types, "BPI17csv", file_type="csv", params=parameters, k=k, weight=weight, sequence=sequence)


def compute_views_for_bpi14(k=7, weight=0.5, sequence=False, selection_method="mmr"):
    filename = '../../data/bpi14/BPIC14.jsonocel'
    object_types = ["ConfigurationItem", "ServiceComponent", "Incident", "Interaction", "Change", "Case_R", "KM"]
    compute_views(filename, object_types, "BPI14", k=k, weight=weight, sequence=sequence)


def compute_views_for_order_management(k=5, weight=0.5, sequence=False, selection_method="mmr"):
    filename = 'data/order-management.jsonocel'
    object_types = ["orders", "items", "packages", "customers", "products"]
    #object_types = ["orders", "packages"]
    compute_views(filename, object_types, "Order", k=5, weight=weight, sequence=sequence)


def get_stats_for_views(filename, selected_views, object_types, edges_leading_types, start_time, method, short_name,
                        file_type="json"):
    path = "results"
    result_json = {}
    result_json["filename"] = filename
    result_json["method"] = method
    result_json["selected_views"] = []
    for k, res_tuple in enumerate(selected_views):
        obj_idx, score, max_sim_to_prev, time = res_tuple
        results_for_k = {}
        obj_t = object_types[obj_idx]
        ocel = load_ocel_by_leading_type(filename, obj_t, file_type=file_type)

        # compute selected views and gather statistics: number of process executions, number of variants,
        # number of events covered, etc.
        # check how difference in weight affects the selected views
        # check how difference between selected views changes with increasing k -> convergence?
        # check how different methods compare to each other

        #interestingness measures by murrilas et al. :
        #  number of traces present in an event log
        # Level of detail (LoD) (Eq. 2): average number of unique activities per trace
        # Average number of events (AE) (Eq. 3): average number of events per trace:
        num_proc_exec = len(ocel.process_executions)
        results_for_k["object_type"] = obj_t
        results_for_k["num_process_executions"] = num_proc_exec
        #results_for_k["num_variants"] = len(ocel.variants())
        results_for_k["num_edges"] = len(edges_leading_types[obj_idx][1])
        results_for_k["score"] = score
        results_for_k["time"] = time - start_time
        results_for_k["position"] = k
        results_for_k["max_sim_to_prev"] = max_sim_to_prev

        events_covered = set()
        for edge in edges_leading_types[obj_idx][1]:
            events_covered.add(edge[0])
            events_covered.add(edge[1])

        number_of_events = sum([len(proc_exec) for proc_exec in ocel.process_executions])
        results_for_k["num_of_events_covered"] = len(events_covered)
        results_for_k["avg_num_of_events_per_trace"] = number_of_events / num_proc_exec if num_proc_exec > 0 else 0
        result_json["selected_views"].append(results_for_k)

    file_id = str(datetime.datetime.now())

    with open(f"{path}/{file_id}_results_{short_name}_{method}.json", "w") as f:
        json.dump(result_json, f)


if __name__ == "__main__":
    main()
