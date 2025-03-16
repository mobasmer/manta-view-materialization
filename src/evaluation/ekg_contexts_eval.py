import argparse
import os
from datetime import datetime
import json
import time
import logging
import duckdb
from promg import DatabaseConnection

from src.strategies.db_mmr_selection import DBRankingSubsetSelector
from src.view_generation.ekg_interacting_entities import compute_indices_by_interacting_entities
from src.view_generation.ekg_leading_type import compute_indices_by_ekg_leading_types

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

    short_name = args.dataset
    temp_db_path = f"data/temp/ekg_leading_types_{short_name}.duckdb"

    neo4j_connection = DatabaseConnection(
        db_name="neo4j",
        uri="bolt://localhost:7687",
        user="neo4j",
        password="12341234")

    compute_views(neo4j_connection, temp_db_path, contextdef=args.contextdef, weight=args.weight, selection_method=args.selection_method,
                  duckdb_config=duckdb_config, short_name=short_name)


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
    parser.add_argument("--contextdef", type=str, default="interact", help="Method for defining context (interact or leading)")
    return parser.parse_args()


# TODO: check that event ids are taken from the event log / assigned deterministically
def compute_views(neo4j_connection, temp_db_path, contextdef="interact", weight=0.5, selection_method="mmr",
              duckdb_config=None, short_name=""):
    start_time = time.time()

    result_file_id = datetime.now().strftime("%Y%m%d-%H%M%S") + "_" + short_name + "_" + selection_method + "_" + "interacting_entities"
    if not relation_indices_precomputed:
        if contextdef == "leading":
            compute_indices_by_ekg_leading_types(neo4j_connection=neo4j_connection, temp_db_path=temp_db_path,
                                                                duckdb_config=duckdb_config, short_name=short_name)
        else:
            compute_indices_by_interacting_entities(neo4j_connection=neo4j_connection, temp_db_path=temp_db_path,
                                                    duckdb_config=duckdb_config)

    with duckdb.connect(temp_db_path) as duckdb_conn:
        view_infos = duckdb_conn.sql("SELECT objecttype FROM viewmeta ORDER BY viewIdx ASC").fetchall()
        context_defs = [view_info[0] for view_info in view_infos]

    indexing_end_time = time.time()
    index_computation_time = indexing_end_time - start_time
    logging.info("Done computing indices by leading type")

    logging.info("Initializing ranking subset selector - computing scores")
    k = len(context_defs)
    ranking_subset_selection = DBRankingSubsetSelector(db_name=temp_db_path, object_types=context_defs,
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
    get_stats_for_views(selected_views, context_defs, temp_db_path, start_time,
                        f"{selection_method}-interacting_entities", result_file_id, runtimes=recorded_times, short_name=short_name)

    if remove_db:
        # Check if the file exists
        if os.path.exists(temp_db_path):
            # Remove the file
            os.remove(temp_db_path)
            print(f"Database '{temp_db_path}' has been removed.")
        else:
            print(f"Database '{temp_db_path}' does not exist.")

        os.path.dirname(temp_db_path)
        temp_edges_path = os.path.join(os.path.dirname(temp_db_path), f"interacting_entities_edges_{short_name}.dbm")
        if os.path.exists(temp_edges_path):
            os.remove(temp_edges_path)
            print(f"Database '{temp_edges_path}' has been removed.")


def get_stats_for_views(selected_views, object_types, db_file, start_time, method, file_id, runtimes=None, short_name=""):
    with duckdb.connect(db_file) as con:
        view_infos = con.sql("SELECT * FROM viewmeta").fetchdf()
        print(view_infos)

    path = "results"
    result_json = {"filename": "neo4j_" + short_name, "method": method, "selected_views": []}

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


    with open(f"{path}/{file_id}_results.json", "w") as f:
        json.dump(result_json, f, indent=4)


if __name__ == "__main__":
    args = parse_args()
    main(args)