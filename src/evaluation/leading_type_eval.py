import argparse
import pickle
from datetime import datetime
import json
import time
import logging

from src.strategies.enumerating_selection import EnumeratingSubsetSelector
from src.strategies.mmr_selection import RankingSubsetSelector
from src.strategies.selection_db import DBRankingSubsetSelector
from src.util.similarity_measures import matching_similarities
from src.view_generation.leading_type_views import load_ocel_by_leading_type, compute_indices_by_leading_type, \
    compute_indices_by_leading_type_parallel
from src.view_generation.leading_type_views_db import compute_indices_by_leading_type_db, \
    compute_indices_by_leading_type_parallel_db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def main(args):
    if args.dataset == "bpi17":
        compute_views_for_bpi17(selection_method=args.selection_method)
    elif args.dataset == "bpi14":
        compute_views_for_bpi14(selection_method=args.selection_method)
    elif args.dataset == "order":
        compute_views_for_order_management(k=2, selection_method=args.selection_method, parallel=True)
    else:
        print("Unknown dataset")

def parse_args():
    parser = argparse.ArgumentParser(description="Compute views for different datasets.")
    parser.add_argument("--dataset", type=str, required=True,
                        help="Dataset to compute views for (bpi17, bpi14, order)")
    parser.add_argument("--k", type=int, default=4, help="Number of views to select")
    parser.add_argument("--weight", type=float, default=0.5, help="Weight for MMR selection")
    parser.add_argument("--sequence", action="store_true", help="Whether to use sequence-based selection")
    parser.add_argument("--selection_method", type=str, default="mmr", help="Selection method (mmr or enumeration)")
    return parser.parse_args()

# TODO: check that event ids are taken from the event log / assigned deterministically
def compute_views(filename, object_types, short_name, file_type="json", params=None, k=2, weight=0.5, sequence=False, selection_method="mmr", parallel=False):
    start_time = time.time()

    if file_type == "csv":
        indices_leading_types = compute_indices_by_leading_type(filename, file_type=file_type, object_types=object_types, act_name=params["act_name"], time_name=params["time_name"], sep=params["sep"])
    else:
        pass
       #indices_leading_types = compute_indices_by_leading_type_parallel(filename, file_type=file_type, object_types=object_types)
       #indices_leading_types = compute_indices_by_leading_type(filename, file_type=file_type,
       #                                                                 object_types=object_types)
       # indices_leading_types = compute_indices_by_leading_type_parallel_db(filename, file_type=file_type, object_types=object_types)
    logging.info("Done computing indices by leading type")

    #for index in indices_leading_types:
    #    obj_type = object_types[index["view_idx"]]
    #    with open(f"data/temp/{short_name}_indices_{obj_type}_{index['view_idx']}.pkl", "wb") as file:
    #        pickle.dump(indices_leading_types, file)

    if selection_method == "mmr":
        logging.info("Initializing ranking subset selector - computing scores")
        #ranking_subset_selection = RankingSubsetSelector(views=indices_leading_types, weight=weight, similarity_function=matching_similarities, parallel=parallel, object_types=object_types)
        ranking_subset_selection = DBRankingSubsetSelector(db_name="leading_type_views.db", object_types=object_types, similarity_function=matching_similarities)
        logging.info("Selecting views by mmr")
        selected_views = ranking_subset_selection.select_view_indices(k)
    else:
        logging.info("Selecting views by enumeration")
        enumerate_subset_selection = EnumeratingSubsetSelector(views=indices_leading_types, similarity_function=matching_similarities)
        selected_views = enumerate_subset_selection.select_view_indices_in_parallel(k)

    print(selected_views)
    logging.info("Computing stats for evaluation")
    get_stats_for_views(filename, selected_views, object_types, indices_leading_types, start_time, f"{selection_method}-leading-type",
                        short_name)

    print(selected_views)

def compute_views_for_bpi17(k=4, weight=0.5, sequence=False, selection_method="mmr", parallel=False):
    filename = 'data/BPIC17.jsonocel'
    object_types = [
        "Application",
        "Workflow",
        "Offer",
        "Case_R"
    ]

    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, "BPI17", k=k, weight=weight, sequence=sequence, selection_method=selection_method)


def compute_views_for_bpi17_csv(k=4, weight=0.5, sequence=False, selection_method="mmr", parallel=False):
    filename = 'data/BPI2017-Final-adapt.csv'
    object_types = ["offer", "application", "event_org:resource", "event_EventID"]

    parameters = {
        "act_name": 'event_activity',
        "time_name": 'event_timestamp',
        "sep": ',',
    }

    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, "BPI17csv", file_type="csv", params=parameters, k=k, weight=weight, sequence=sequence, selection_method=selection_method, parallel=parallel)


def compute_views_for_bpi14(k=7, weight=0.5, sequence=False, selection_method="mmr", parallel=False):
    filename = 'data/BPIC14.jsonocel'
    object_types = ["ConfigurationItem", "ServiceComponent", "Incident", "Interaction", "Change", "Case_R", "KM"]

    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, "BPI14", k=k, weight=weight, sequence=sequence, selection_method=selection_method, parallel=parallel)


def compute_views_for_order_management(k=5, weight=0.5, sequence=False, selection_method="mmr", parallel=False):
    filename = 'data/order-management.jsonocel'
    object_types = ["orders", "items", "packages", "customers", "products"]
    #object_types = ["customers", "products"]#, "packages"]

    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, "Order", k=k, weight=weight, sequence=sequence, selection_method=selection_method, parallel=parallel)


def get_stats_for_views(filename, selected_views, object_types, indices_leading_types, start_time, method, short_name):
    path = "results"
    result_json = {}
    result_json["filename"] = filename
    result_json["method"] = method
    result_json["selected_views"] = []
    for k, res_tuple in enumerate(selected_views):
        obj_idx, score, score_info, finish_time = res_tuple
        results_for_k = {}
        obj_t = object_types[obj_idx]

        # compute selected views and gather statistics: number of process executions, number of variants,
        # number of events covered, etc.
        # check how difference in weight affects the selected views
        # check how difference between selected views changes with increasing k -> convergence?
        # check how different methods compare to each other

        #interestingness measures by murrilas et al. :
        #  number of traces present in an event log
        # Level of detail (LoD) (Eq. 2): average number of unique activities per trace
        # Average number of events (AE) (Eq. 3): average number of events per trace:
        # TODO: add level of detail: average number of unique activities per trace
        results_for_k["object_type"] = obj_t
        results_for_k["num_process_executions"] = indices_leading_types[obj_idx]["num_proc_exec"]
        #results_for_k["num_variants"] = len(ocel.variants())
        results_for_k["num_edges"] = len(indices_leading_types[obj_idx]["relation_index"])
        results_for_k["score info"] = score_info
        results_for_k["time"] = finish_time - start_time
        results_for_k["position"] = k

        events_covered = set()
        for edge in indices_leading_types[obj_idx]["relation_index"]:
            events_covered.add(edge[0])
            events_covered.add(edge[1])

        results_for_k["num_of_events_covered"] = len(events_covered)
        results_for_k["num_of_events_total-dupl"] = indices_leading_types[obj_idx]["num_of_events"] # incl duplicates events
        results_for_k["avg_num_of_events_per_trace"] = indices_leading_types[obj_idx]["avg_num_of_events_per_trace"]
        result_json["selected_views"].append(results_for_k)

    now = datetime.now()
    file_id = now.strftime("%Y-%m-%d %H:%M:%S")

    with open(f"{path}/{file_id}_results_{short_name}_{method}.json", "w") as f:
        json.dump(result_json, f, indent=4)


if __name__ == "__main__":
    args = parse_args()
    main(args)
