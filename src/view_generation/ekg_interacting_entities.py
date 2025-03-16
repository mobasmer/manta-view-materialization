import csv
import itertools
import logging
import os
import tempfile
import dbm

import duckdb

from src.util.ekg_queries import get_entity_types_query, get_contexts_query_single_object, get_object_pairs_query, \
    get_events_for_objects_query, event_time_attr, entity_id_attr, entity_type_attr
from src.util.query_result_parser import parse_to_list



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
#
# def main():
#     short_name = "order"
#     temp_db_path = f"data/temp/interacting_entities_{short_name}.duckdb"
#
#     neo4j_connection = DatabaseConnection(
#         db_name="neo4j",
#         uri="bolt://localhost:7687",
#         user="neo4j",
#         password="12341234")
#
#     compute_indices_by_interacting_entities(neo4j_connection, temp_db_path, short_name)


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

    os.path.dirname(temp_db_path)
    temp_edges_path = os.path.join(os.path.dirname(temp_db_path), f"interacting_entities_edges_{short_name}.dbm")
    with duckdb.connect(temp_db_path, config=config) as duckdb_conn,\
        dbm.open(temp_edges_path, 'c') as edges_db:

        duckdb_conn.sql("DROP TABLE IF EXISTS viewmeta")
        duckdb_conn.sql(
            "CREATE TABLE IF NOT EXISTS viewmeta(viewIdx INTEGER, objecttype STRING, numProcExecs INTEGER, numEvents INTEGER, AvgNumEventsPerTrace FLOAT)")

        #duckdb_conn.sql("DROP TABLE IF EXISTS edges")
        #duckdb_conn.sql("CREATE TABLE IF NOT EXISTS edges(source INTEGER, target INTEGER, edgeId INTEGER primary key)")

        for context_name in context_names:
            duckdb_conn.sql("DROP TABLE IF EXISTS " + context_name)
            duckdb_conn.sql("CREATE TABLE IF NOT EXISTS " + context_name + "(edge INTEGER, procExec String)")
        duckdb_conn.commit()

        # edges = {}
        incr_idx = 0
        for cidx, context_def in enumerate(context_defs):
            logging.info(f"Start building relation index for {context_names[cidx]}")
            compute_relation_index(neo4j_connection, context_def, context_names[cidx], cidx, duckdb_conn, incr_idx, edges_db)

            duckdb_conn.sql("CREATE INDEX IF NOT EXISTS " + context_names[cidx] + "_edge_index ON " + context_names[cidx] + "(edge)")
            duckdb_conn.commit()

            logging.info(f"Finished building relation index for {context_names[cidx]}")


def compute_relation_index(neo4j_connection, context_def, context_name, cidx, duckdb_conn, incr_idx, edges):
    ot1, ot2 = context_def
    edge2obj = []
    batch_size = 50000
    i = 0
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', dir='data/temp')
    temp_file.close()

    if ot2 is None:
        logging.info("start context query for %s", context_name)
        query_result = neo4j_connection.exec_query(get_contexts_query_single_object, **{"ot1": ot1})
        num_proc_execs = len(query_result)
        num_events = sum([len(r['eventList']) for r in query_result])
        avg_num_events_per_trace =  num_events / num_proc_execs if num_proc_execs > 0 else 0

        for pi_idx, record in enumerate(query_result):
            events = record['eventList']
            for j in range(len(events) - 1):
                if i == batch_size:
                    with open(temp_file.name, 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerows(edge2obj)
                    edge2obj = []
                    i = 0
                edge = str((events[j]["id"], events[j + 1]["id"]))
                if edge not in edges:
                    edges[edge] = str(incr_idx)
                    incr_idx += 1
                edge2obj.append((int(edges[edge]), pi_idx))
                i += 1

        if len(edge2obj) > 0:
            with open(temp_file.name, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(edge2obj)

        duckdb_conn.sql(f"COPY {context_name} FROM '{temp_file.name}' (DELIMITER ',')")
        duckdb_conn.commit()

        temp_file.close()
        os.remove(temp_file.name)

    else:
        logging.info("start context query for %s", context_name)
        obj_pair_result = neo4j_connection.exec_query(get_object_pairs_query, **{"ot1": ot1, "ot2": ot2})
        num_proc_execs = len(obj_pair_result)
        num_events = 0
        for pi_idx, obj_pair in enumerate(obj_pair_result):
            query_result = neo4j_connection.exec_query(get_events_for_objects_query, **{"o1": obj_pair["o1"], "o2": obj_pair["o1"]})
            num_events += len(query_result[0]['eventList'])
            events = query_result[0]['eventList']
            for j in range(len(events) - 1):
                if i == batch_size:
                    with open(temp_file.name, 'a', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerows(edge2obj)
                    edge2obj = []
                    i = 0
                edge = str((events[j]["id"], events[j + 1]["id"]))
                if edge not in edges:
                    edges[edge] = str(incr_idx)
                    incr_idx += 1
                edge2obj.append((int(edges[edge]), pi_idx))
                i += 1

        avg_num_events_per_trace = num_events / num_proc_execs if num_proc_execs > 0 else 0

        if len(edge2obj) > 0:
            with open(temp_file.name, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(edge2obj)

        if len(obj_pair_result) > 0:
            duckdb_conn.sql(f"COPY {context_name} FROM '{temp_file.name}' (DELIMITER ',')")
            duckdb_conn.commit()

        temp_file.close()
        os.remove(temp_file.name)

    duckdb_conn.execute("INSERT INTO viewmeta VALUES (?, ?, ?, ?, ?)",
                (cidx, context_name, num_proc_execs, num_events, avg_num_events_per_trace))
    duckdb_conn.commit()

    logging.info("Ingested relation index")

#if __name__:
#    main()