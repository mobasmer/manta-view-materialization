import argparse
import os
from datetime import datetime
import json
import time
import logging

import duckdb

from src.strategies.db_mmr_selection import DBRankingSubsetSelector
from src.view_generation.leading_type_views_db import compute_indices_by_leading_type_db
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

relation_indices_precomputed = True
counts_precomputed = False
remove_db = True

def main(args):
    if args.dataset == "bpi17":
        compute_views_for_bpi17(selection_method=args.selection_method, max_mem=args.maxmem, threads=args.threads)
    elif args.dataset == "bpi14":
        compute_views_for_bpi14(selection_method=args.selection_method, max_mem=args.maxmem, threads=args.threads)
    elif args.dataset == "order":
        compute_views_for_order_management(k=5, max_mem=args.maxmem, threads=args.threads)
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
    parser.add_argument("--maxmem", type=str, default="4GB", help="Max available memory for DuckDB (must be KB, MB, GB)")
    parser.add_argument("--threads", type=int, default=4, help="Max number of threads for DuckDB")
    return parser.parse_args()

# TODO: check that event ids are taken from the event log / assigned deterministically
def compute_views(filename, object_types, short_name, file_type="json", params=None, k=2, weight=0.5, selection_method="mmr", max_mem="4GB", threads=4):
    start_time = time.time()
    db_name = f"leading_type_views_{short_name}.db"
    if not relation_indices_precomputed:
        compute_indices_by_leading_type_db(filename, db_name, file_type=file_type, object_types=object_types, max_duckdb_mem=max_mem, max_duckdb_threads=threads)
    logging.info("Done computing indices by leading type")


    logging.info("Initializing ranking subset selector - computing scores")
    ranking_subset_selection = DBRankingSubsetSelector(db_name=db_name, object_types=object_types,
                                                      counts_precomputed=counts_precomputed, weight=weight,
                                                       max_duckdb_mem=max_mem, max_duckdb_threads=threads)
    logging.info("Selecting views by mmr")
    selected_views = ranking_subset_selection.select_view_indices(k)
    end_time = time.time()

    print(selected_views)
    logging.info("Computing stats for evaluation")
    get_stats_for_views(filename, selected_views, object_types, db_name, start_time,
                        f"{selection_method}-leading-type", short_name, runtime=end_time-start_time)

    print(selected_views)

    if remove_db:
        # Check if the file exists
        if os.path.exists(db_name):
            # Remove the file
            os.remove(db_name)
            print(f"Database '{db_name}' has been removed.")
        else:
            print(f"Database '{db_name}' does not exist.")

def compute_views_for_bpi17(k=4, weight=0.5,selection_method="mmr", max_mem="4GB", threads=4):
    filename = 'data/BPIC17.jsonocel'
    object_types = [
        "Application",
        "Workflow",
        "Offer",
        "Case_R"
    ]

    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, "BPI17", k=k, weight=weight, selection_method=selection_method, max_mem=max_mem, threads=threads)


def compute_views_for_bpi17_csv(k=4, weight=0.5, selection_method="mmr", max_mem="4GB", threads=4):
    filename = 'data/BPI2017-Final-adapt.csv'
    object_types = ["offer", "application", "event_org:resource", "event_EventID"]

    parameters = {
        "act_name": 'event_activity',
        "time_name": 'event_timestamp',
        "sep": ',',
    }

    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, "BPI17csv", file_type="csv", params=parameters, k=k, weight=weight, selection_method=selection_method, max_mem=max_mem, threads=threads)


def compute_views_for_bpi14(k=7, weight=0.5, selection_method="mmr", max_mem="4GB", threads=4):
    filename = 'data/BPIC14.jsonocel'
    object_types = ["ConfigurationItem", "ServiceComponent", "Incident", "Interaction", "Change", "Case_R", "KM"]

    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, "BPI14", k=k, weight=weight, selection_method=selection_method, max_mem=max_mem, threads=threads)


def compute_views_for_order_management(k=5, weight=0.5, selection_method="mmr", max_mem="4GB", threads=4):
    filename = 'data/order-management.jsonocel'
    object_types = ["orders", "items", "packages", "customers", "products"]
    #object_types = ["customers", "products", "packages"]

    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, "Order", k=k, weight=weight, selection_method=selection_method, max_mem=max_mem, threads=threads)


def get_stats_for_views(filename, selected_views, object_types, db_name, start_time, method, short_name, runtime=None):
    view_infos = None
    with duckdb.connect(db_name) as con:
        view_infos = con.sql("SELECT * FROM viewmeta").fetchdf()
        print(view_infos)
        # viewIdx INTEGER, objecttype STRING, numProcExecs INTEGER, numEvents INTEGER, AvgNumEventsPerTrace FLOAT

    path = "results"
    result_json = {}
    result_json["filename"] = filename
    result_json["method"] = method
    if runtime is not None:
        result_json["runtime"] = runtime
    result_json["selected_views"] = []
    for k, res_tuple in enumerate(selected_views):
        obj_idx, score, score_info, finish_time = res_tuple
        results_for_k = {}
        obj_t = object_types[obj_idx]

        with duckdb.connect(db_name) as con:
            num_edges = con.sql("SELECT COUNT(DISTINCT edge) FROM " + obj_t).fetchone()[0]

        # compute selected views and gather statistics: number of process executions, number of variants,
        # number of events covered, etc.
        # check how difference in weight affects the selected views
        # check how difference between selected views changes with increasing k -> convergence?
        # check how different methods compare to each other

        # TODO: add level of detail: average number of unique activities per trace (Murillas et al., 2019)
        # TODO: add covered events covered
        results_for_k["object_type"] = obj_t
        results_for_k["num_process_executions"] = int(view_infos.loc[view_infos['viewIdx'] == obj_idx, 'numProcExecs'].values[0])
            # number of traces present in an event log (Murillas et al., 2019)
        results_for_k["num_edges"] = num_edges
        results_for_k["score info"] = score_info
        results_for_k["time"] = finish_time - start_time
        results_for_k["position"] = k

        #events_covered = set()
        #for edge in indices_leading_types[obj_idx]["relation_index"]:
        #    events_covered.add(edge[0])
        #    events_covered.add(edge[1])

        #results_for_k["num_of_events_covered"] = len(events_covered)
        results_for_k["num_of_events_total-dupl"] = int(view_infos.loc[view_infos['viewIdx'] == obj_idx, 'numEvents'].values[0]) # incl duplicates events
        results_for_k["avg_num_of_events_per_trace"] = float(view_infos.loc[view_infos['viewIdx'] == obj_idx, 'AvgNumEventsPerTrace'].values[0])
            # Average number of events (AE) (Eq. 3): average number of events per trace (Murillas et al., 2019)
        result_json["selected_views"].append(results_for_k)

    now = datetime.now()
    file_id = now.strftime("%Y-%m-%d %H:%M:%S")

    with open(f"{path}/{file_id}_results_{short_name}_{method}.json", "w") as f:
        json.dump(result_json, f, indent=4)


if __name__ == "__main__":
    args = parse_args()
    main(args)
