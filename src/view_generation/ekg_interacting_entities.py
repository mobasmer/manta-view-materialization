import csv
import itertools
import logging
import os
import tempfile
import dbm

import duckdb

from src.util.ekg_queries import get_entity_types_query, get_contexts_query_single_object, get_object_pairs_query, \
    get_events_for_objects_query, entity_type_attr, get_object_pairs_query_iterative
from src.util.query_result_parser import parse_to_list


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

incr_edge_idx = 0
incr_context_idx = 0

def compute_indices_by_interacting_entities(neo4j_connection, temp_db_path, short_name="", duckdb_config=None, entity_id_attr="id"):
    result = neo4j_connection.exec_query(get_entity_types_query)
    entity_types = parse_to_list(result, "e." + entity_type_attr)

    context_defs = []
    context_defs = [(t,None) for t in entity_types]
    context_defs.extend(list(itertools.combinations(entity_types, 2)))
    context_defs.extend([(t, t) for t in entity_types])
    context_names = [str(t) if t2 is None else str(t + "___" + t2) for t, t2 in context_defs]



    config = {}
    if duckdb_config is not None:
        if "memory_limit" in duckdb_config:
            config["memory_limit"] = duckdb_config["memory_limit"]
        if "threads" in duckdb_config:
            config["threads"] = duckdb_config["threads"]


    temp_edges_path = os.path.join(os.path.dirname(temp_db_path), f"interacting_entities_edges_{short_name}.dbm")
    with duckdb.connect(temp_db_path, config=config) as duckdb_conn:#,\
        #dbm.open(temp_edges_path, 'c') as edges_db:

        duckdb_conn.sql("DROP TABLE IF EXISTS viewmeta")
        duckdb_conn.sql(
            "CREATE TABLE IF NOT EXISTS viewmeta(viewIdx INTEGER, objecttype STRING, numProcExecs INTEGER, numEvents INTEGER, AvgNumEventsPerTrace FLOAT)")

        #duckdb_conn.sql("DROP TABLE IF EXISTS edges")
        #duckdb_conn.sql("CREATE TABLE IF NOT EXISTS edges(source INTEGER, target INTEGER, edgeId INTEGER primary key)")

        edges_db = {}

        for i, context_def in enumerate(context_defs):
            logging.info(f"Start building relation index for {context_names[i]}")
            compute_relation_index(neo4j_connection, context_def, context_names[i], duckdb_conn, edges_db)
            logging.info(f"Finished building relation index for {context_names[i]}")


def compute_relation_index(neo4j_connection, context_def, context_name, duckdb_conn, edges):
    global incr_edge_idx
    global incr_context_idx
    ot1, ot2 = context_def
    edge2obj = []
    batch_size = 100000
    i = 0
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', dir='data/temp')
    temp_file.close()

    if ot2 is None:
        logging.info("start context query for %s", context_name)
        query_result = neo4j_connection.exec_query(get_contexts_query_single_object, **{"ot1": ot1})
        num_proc_execs = len(query_result)
        num_events = sum([len(r['eventList']) for r in query_result])

        for pi_idx, record in enumerate(query_result):
            events = record['eventList']
            for j in range(len(events) - 1):
                if i == batch_size:
                    with open(temp_file.name, 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerows(edge2obj)
                    edge2obj = []
                    i = 0
                #edge = str((events[j]["id"], events[j + 1]["id"]))
                edge = (events[j]["id"], events[j + 1]["id"])
                if edge not in edges:
                    #edges[edge] = str(incr_idx)
                    edges[edge] = incr_edge_idx
                    incr_edge_idx += 1
                edge2obj.append((edges[edge], pi_idx))
                i += 1

        logging.info("Finished context query for %s", context_name)

    else:
        logging.info("start context query for %s", context_name)
        #obj_pair_result = neo4j_connection.exec_query(get_object_pairs_query, **{"ot1": ot1, "ot2": ot2})
        obj_pairs = set()
        for path_length in range(1,11):
            obj_pair_result = neo4j_connection.exec_query(get_object_pairs_query_iterative, **{"ot1": ot1, "ot2": ot2, "path_length": path_length})
            obj_pairs.update([(record["o1"], record["o2"]) for record in obj_pair_result])
        num_proc_execs = len(obj_pairs)
        num_events = 0
        logging.info("Collecting contexts for %s", context_name)
        #obj_pairs = [[o1, o2] for o1, o2 in obj_pairs]
        #obj_pairs = [(record["o1"], record["o2"]) for record in obj_pair_result]
        #obj_pair_events = neo4j_connection.exec_query(get_events_for_many_object_pairs_query, **{"obj_pairs": obj_pairs})
        logging.info("query done")
        for pi_idx, obj_pair in enumerate(obj_pairs):
            o1, o2 = obj_pair
            #query_result = neo4j_connection.exec_query(get_events_for_objects_query, **{"o1": obj_pair["o1"], "o2": obj_pair["o1"]})
            query_result = neo4j_connection.exec_query(get_events_for_objects_query, **{"o1": o1, "o2": o2})
            events = query_result[0]['eventList']
        #for pi_idx, obj_pair_res in enumerate(obj_pair_events):
        #    events = obj_pair_res['eventList']
            num_events += len(events)
            for j in range(len(events) - 1):
                if i == batch_size:
                    with open(temp_file.name, 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerows(edge2obj)
                    edge2obj = []
                    i = 0

                    if os.path.getsize(temp_file.name) > 50000000000:
                        duckdb_conn.close()
                        temp_file.close()
                        #edges.close()
                        raise Exception("Relation index too large")

                #edge = str((events[j]["id"], events[j + 1]["id"]))
                edge = (events[j]["id"], events[j + 1]["id"])
                if edge not in edges:
                    #edges[edge] = str(incr_edge_idx)
                    edges[edge] = incr_edge_idx
                    incr_edge_idx += 1
                edge2obj.append((edges[edge], pi_idx))
                i += 1
        logging.info("Collected contexts for %s", context_name)

    # write remaining edges
    if len(edge2obj) > 0:
        with open(temp_file.name, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(edge2obj)

    # only store non-empty views
    if num_proc_execs > 0:
        # create db table
        duckdb_conn.sql("DROP TABLE IF EXISTS " + context_name)
        duckdb_conn.sql("CREATE TABLE IF NOT EXISTS " + context_name + "(edge INTEGER, procExec String)")

        # store meta information on view, esp. cidx and name for reuse in scoring
        avg_num_events_per_trace = num_events / num_proc_execs if num_proc_execs > 0 else 0
        duckdb_conn.execute("INSERT INTO viewmeta VALUES (?, ?, ?, ?, ?)",
                            (incr_context_idx, context_name, num_proc_execs, num_events, avg_num_events_per_trace))
        duckdb_conn.commit()

        # only counting indices for non-empty views, to match indices for list of views later on
        incr_context_idx += 1

        # transfer entries from temp csv file to corresponding duck db table
        duckdb_conn.sql(f"COPY {context_name} FROM '{temp_file.name}' (DELIMITER ',')")
        duckdb_conn.commit()

        #create index on edge column for join later on
        duckdb_conn.sql(
            "CREATE INDEX IF NOT EXISTS " + context_name + "_edge_index ON " + context_name + "(edge)")
        duckdb_conn.commit()

    temp_file.close()
    os.remove(temp_file.name)

    logging.info("Ingested relation index for context %s", context_name)
