import argparse
import os
from datetime import datetime
import json
import time
import logging

import duckdb

from src.strategies.db_mmr_selection import DBRankingSubsetSelector
from src.util.filter_log import filter_ocel_json, load_ocel_from_file
from src.view_generation.ocel_leading_type import compute_indices_by_leading_type_db

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
        if args.filterdate:
            compute_views_for_bpi14(duckdb_config=duckdb_config, filter_date=args.filterdate)
        else:
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
    parser.add_argument("--dbpath", type=str, default=None, help="Max available memory for DuckDB (must be KB, MB, GB)")
    parser.add_argument("--filterdate", type=str, default="2013-09-30T23:59:59", help="Filter date for BPI14")
    return parser.parse_args()


# TODO: check that event ids are taken from the event log / assigned deterministically
def compute_views(filename, object_types, db_name, file_type="json", k=2, weight=0.5, selection_method="mmr",
              duckdb_config=None, short_name=""):
    start_time = time.time()

    result_file_id = datetime.now().strftime("%Y%m%d-%H%M%S")
    if not relation_indices_precomputed:
        compute_indices_by_leading_type_db(filename, db_name, file_type=file_type, object_types=object_types,
                                           duckdb_config=duckdb_config)
    indexing_end_time = time.time()
    index_computation_time = indexing_end_time - start_time
    logging.info("Done computing indices by leading type (ocel) in " + str(index_computation_time) + " seconds")

    logging.info("Initializing ranking subset selector - computing scores")
    ranking_subset_selection = DBRankingSubsetSelector(db_name=db_name, object_types=object_types,
                                                       counts_precomputed=counts_precomputed, weight=weight,
                                                       duckdb_config=duckdb_config, file_id=result_file_id)
    score_comp_end_time = time.time()
    score_computation_time = score_comp_end_time - indexing_end_time
    logging.info("Done scoring views in " + str(score_computation_time) + " seconds")

    logging.info("Selecting views by mmr")
    selected_views = ranking_subset_selection.select_view_indices(k)
    view_selection_time = time.time() - score_comp_end_time
    logging.info("Done selecting views in " + str(view_selection_time) + " seconds")
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


def compute_views_for_bpi17(k=None, weight=0.5, selection_method="mmr", duckdb_config=None):
    filename = 'data/BPIC17.jsonocel'
    object_types = [
        "Application",
        "Workflow",
        "Offer",
        "Case_R"
    ]
    k = len(object_types) if k is None else k

    db_file = db_path + "leading_type_views_BPI17.duckdb"

    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, db_file, k=k, weight=weight, selection_method=selection_method,
                  duckdb_config=duckdb_config, short_name="BPI17")


def compute_views_for_bpi17_csv(k=None, weight=0.5, selection_method="mmr", duckdb_config=None):
    filename = 'data/BPI2017-Final-adapt.csv'
    object_types = ["offer", "application", "event_org:resource", "event_EventID"]
    k = len(object_types) if k is None else k

    parameters = {
        "act_name": 'event_activity',
        "time_name": 'event_timestamp',
        "sep": ',',
    }

    db_file = db_path + "leading_type_views_BPI17csv.duckdb"

    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, db_file, file_type="csv", k=k, weight=weight,
                  selection_method=selection_method, duckdb_config=duckdb_config, short_name="BPI17csv")


def compute_views_for_bpi14(k=None, weight=0.5, selection_method="mmr", duckdb_config=None, filter_date="2013-09-30T23:59:59"):
    # data = load_ocel_from_file("data/order-management.jsonocel")
    data = load_ocel_from_file("data/BPIC14.jsonocel.zip")
    filtered_data = filter_ocel_json(data, start_time="2013-01-01T00:00:01", end_time=filter_date)

    filename = f'data/bpi14-filtered-{filter_date.split("T")[0]}.jsonocel'
    with open(filename, "w") as f:
        f.write(json.dumps(filtered_data, indent=4))

    object_types = ["ConfigurationItem", "ServiceComponent", "Incident", "Interaction", "Change", "Case_R", "KM"]
    k = len(object_types) if k is None else k
   # db_file = db_path + f"leading_type_views_bpi14-filtered-{filter_date.split('T')[0]}.duckdb"
    db_file = db_path + f"leading_type_views_bpi14-filtered.duckdb"
    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, db_file, k=k, weight=weight, selection_method=selection_method,
                  duckdb_config=duckdb_config, short_name="BPI14")


def compute_views_for_order_management(k=None, weight=0.5, selection_method="mmr", duckdb_config=None):
    filename = 'data/order-management.jsonocel'
    object_types = ["orders", "items", "packages", "customers", "products"]
    #object_types = ["packages", "customers", "products"]
    k = len(object_types) if k is None else k
    db_file = db_path + "leading_type_views_order.duckdb"
    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, db_file, k=k, weight=weight, selection_method=selection_method,
                  duckdb_config=duckdb_config, short_name="order")

def compute_views_for_bpi15(k=None, weight=0.5, selection_method="mmr", duckdb_config=None, lognr="1"):
    filename = 'data/BPIC15_Municipality' + lognr + '.jsonocel'
    object_types = ["Application",
      "Case_R",
      "Responsible_actor",
      "monitoringResource"]

    k = len(object_types) if k is None else k
    db_file = db_path + "leading_type_views_bpi15_"+ lognr +".duckdb"
    assert k <= len(object_types), "k must be less than the number of object types"
    compute_views(filename, object_types, db_file, k=k, weight=weight, selection_method=selection_method,
                  duckdb_config=duckdb_config, short_name="BPI15_" + lognr)


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


if __name__ == "__main__":
    args = parse_args()
    main(args)
