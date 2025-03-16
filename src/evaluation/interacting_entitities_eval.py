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

relation_indices_precomputed = False
counts_precomputed = False
remove_db = False
db_path = "data/temp/"

def main(args):
    duckdb_config = {}
    if args.maxmem is not None:
        duckdb_config["memory_limit"] = args.maxmem
    if args.threads is not None:
        duckdb_config["threads"] = args.threads
    if args.dbpath is not None:
        global db_path
        db_path = args.dbpath

    if args.dataset == "bpi17":
        compute_views_for_bpi17(duckdb_config=duckdb_config)
    elif args.dataset == "bpi14":
        compute_views_for_bpi14(duckdb_config=duckdb_config)
    elif args.dataset == "order":
        compute_views_for_order_management(duckdb_config=duckdb_config)
    elif args.dataset.startswith("bpi15"):
        lognr = args.dataset.split("-")[1]
        compute_views_for_bpi15(duckdb_config=duckdb_config, lognr = lognr)
    else:
        print("Unknown dataset")


def parse_args():
    parser = argparse.ArgumentParser(description="Compute views for different datasets.")
    parser.add_argument("--dataset", type=str, required=True,
                        help="Dataset to compute views for (bpi17, bpi14, order)")
    parser.add_argument("--k", type=int, default=4, help="Number of views to select")
    parser.add_argument("--weight", type=float, default=0.5, help="Weight for MMR selection")
    parser.add_argument("--selection_method", type=str, default="mmr", help="Selection method (mmr or enumeration)")
    parser.add_argument("--maxmem", type=str, default=None, help="Max available memory for DuckDB (must be KB, MB, GB)")
    parser.add_argument("--threads", type=int, default=None, help="Max number of threads for DuckDB")
    parser.add_argument("--dbpath", type=str, default=None, help="Path for temporary database files")
    return parser.parse_args()


# TODO: check that event ids are taken from the event log / assigned deterministically
def compute_views(filename, object_types, db_name, file_type="json", k=2, weight=0.5, selection_method="mmr",
              duckdb_config=None, short_name=""):
    start_time = time.time()

    result_file_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    if not relation_indices_precomputed:
        compute_indices_by_interacting_entities(filename, db_name, file_type=file_type, object_types=object_types,
                                           duckdb_config=duckdb_config)
    indexing_end_time = time.time()
    index_computation_time = indexing_end_time - start_time
    logging.info("Done computing indices by leading type")

    logging.info("Initializing ranking subset selector - computing scores")
    ranking_subset_selection = DBRankingSubsetSelector(db_name=db_name, object_types=object_types,
                                                       counts_precomputed=counts_precomputed, weight=weight,
                                                       duckdb_config=duckdb_config, file_id=result_file_id)
    score_comp_end_time = time.time()
    score_computation_time = score_comp_end_time - indexing_end_time

    logging.info("Selecting views by mmr")
    selected_views = ranking_subset_selection.select_view_indices(k)
    view_selection_time = time.time() - score_comp_end_time
    run_time = time.time() - start_time

    recorded_times = {
        "index_computation_time": index_computation_time,
        "score_computation_time": score_computation_time,
        "view_selection_time": view_selection_time,
        "run_time": run_time
    }

    print(selected_views)
    logging.info("Computing stats for evaluation")
    get_stats_for_views(filename, selected_views, object_types, db_name, start_time,
                        f"{selection_method}-leading-type", result_file_id, runtimes=recorded_times, short_name=short_name)

    if remove_db:
        # Check if the file exists
        if os.path.exists(db_name):
            # Remove the file
            os.remove(db_name)
            print(f"Database '{db_name}' has been removed.")
        else:
            print(f"Database '{db_name}' does not exist.")

def get_stats_for_views(filename, selected_views, object_types, db_file, start_time, method, file_id, runtimes=None, short_name=""):
    with duckdb.connect(db_file) as con:
        view_infos = con.sql("SELECT * FROM viewmeta").fetchdf()
        print(view_infos)

    path = "results"
    result_json = {"filename": filename, "method": method, "selected_views": []}
    if runtimes is not None:
        result_json["runtimes"] = runtimes

    for k, res_tuple in enumerate(selected_views):
        obj_idx, score, score_info, finish_time = res_tuple
        results_for_k = {}
        obj_t = object_types[obj_idx]

        with duckdb.connect(db_file) as con:
            num_edges = con.sql("SELECT COUNT(DISTINCT edge) FROM " + obj_t).fetchone()[0]

        # compute selected views and gather statistics: number of process executions, number of variants,
        # number of events covered, etc.
        # check how difference in weight affects the selected views
        # check how difference between selected views changes with increasing k -> convergence?
        # check how different methods compare to each other

        # TODO: add level of detail: average number of unique activities per trace (Murillas et al., 2019)
        # TODO: add covered events covered
        results_for_k["object_type"] = obj_t
        results_for_k["num_process_executions"] = int(
            view_infos.loc[view_infos['viewIdx'] == obj_idx, 'numProcExecs'].values[0])
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
        results_for_k["num_of_events_total-dupl"] = int(
            view_infos.loc[view_infos['viewIdx'] == obj_idx, 'numEvents'].values[0])  # incl duplicates events
        results_for_k["avg_num_of_events_per_trace"] = float(
            view_infos.loc[view_infos['viewIdx'] == obj_idx, 'AvgNumEventsPerTrace'].values[0])
        # Average number of events (AE) (Eq. 3): average number of events per trace (Murillas et al., 2019)
        result_json["selected_views"].append(results_for_k)

    now = datetime.now()

    with open(f"{path}/{file_id}_results_{short_name}_{method}.json", "w") as f:
        json.dump(result_json, f, indent=4)