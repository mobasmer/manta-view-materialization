import csv
import dbm
import logging
import os
import tempfile

import duckdb

from src.util.ekg_queries import \
    get_leading_type_query, get_process_instances_multiple_objects, get_objects_for_leading_type, \
    get_objects_for_leading_type_object_iteratively, get_entity_types_query, entity_type_attr, \
    get_objects_for_leading_type_object_union
from src.util.query_result_parser import parse_to_list


incr_edge_idx = 0

def compute_indices_by_ekg_leading_types(neo4j_connection, temp_db_path, short_name="", duckdb_config=None, max_path_length=1000):
    result = neo4j_connection.exec_query(get_entity_types_query)
    entity_types = parse_to_list(result, "e." + entity_type_attr)

    config = {}
    if duckdb_config is not None:
        if "memory_limit" in duckdb_config:
            config["memory_limit"] = duckdb_config["memory_limit"]
        if "threads" in duckdb_config:
            config["threads"] = duckdb_config["threads"]

    #temp_edges_path = os.path.join(os.path.dirname(temp_db_path), f"ekg_leading_types_edges_{short_name}.dbm")
    with duckdb.connect(temp_db_path, config=config) as duckdb_conn: #, \
           # dbm.open(temp_edges_path, 'c') as edges_db:

        duckdb_conn.sql("DROP TABLE IF EXISTS viewmeta")
        duckdb_conn.sql(
            "CREATE TABLE IF NOT EXISTS viewmeta(viewIdx INTEGER, objecttype STRING, numProcExecs INTEGER, numEvents INTEGER, AvgNumEventsPerTrace FLOAT)")

        # duckdb_conn.sql("DROP TABLE IF EXISTS edges")
        # duckdb_conn.sql("CREATE TABLE IF NOT EXISTS edges(source INTEGER, target INTEGER, edgeId INTEGER primary key)")

        for context_name in entity_types:
            duckdb_conn.sql("DROP TABLE IF EXISTS " + context_name)
            duckdb_conn.sql("CREATE TABLE IF NOT EXISTS " + context_name + "(edge INTEGER, procExec String)")
        duckdb_conn.commit()

        edges_db = {}
        for cidx, entity_type in enumerate(entity_types):
            logging.info("Computing leading type context for %s", entity_type)
            compute_leading_type_context_iteratively(cidx, entity_type, neo4j_connection, duckdb_conn, edges_db, max_path_length=max_path_length, entity_types=entity_types)
            #compute_leading_type_context_union(i, entity_type, neo4j_connection, duckdb_conn, edges_db,
            #                                         max_path_length=10, entity_types=entity_types)
            duckdb_conn.sql("CREATE INDEX IF NOT EXISTS " + entity_type + "_edge_index ON " + entity_type + "(edge)")
            duckdb_conn.commit()

def compute_leading_type_context_iteratively(cidx, ot1, neo4j_connection, duckdb_conn, edges_db, max_path_length=10, entity_types=None):
    query_results = neo4j_connection.exec_query(get_objects_for_leading_type, **{"ot1": ot1})
    contexts4leading = []
    for record in query_results:
        objId = record['id']
        context = [objId]
        types_seen_distance = {}

        logging.info("start query for %s", objId)

        for i in range(max_path_length):
            result = neo4j_connection.exec_query(get_objects_for_leading_type_object_iteratively, **{"objId": objId, "path_length": i})
            #print(result)
            for record in result:
                if record['entType'] not in types_seen_distance:
                    types_seen_distance[record['entType']] = i
                    context.append(record['ent2Id'])
                else:
                    if i <= types_seen_distance[record['entType']]:
                        context.append(record['ent2Id'])
            if entity_types is not None:
                if all([ot in types_seen_distance for ot in entity_types]):
                    break
        logging.info("finished queries for %s", objId)
        contexts4leading.append(context)

    compute_relation_index(contexts4leading, neo4j_connection, duckdb_conn, cidx, ot1, edges_db)

def compute_leading_type_context_union(cidx, ot1, neo4j_connection, duckdb_conn, edges_db, max_path_length=1000, entity_types=None):
    query_results = neo4j_connection.exec_query(get_objects_for_leading_type, **{"ot1": ot1})
    contexts4leading = []
    for record in query_results:
        #print(record)
        objId = record['id']
        logging.info("start query for %s", objId)
        result = neo4j_connection.exec_query(get_objects_for_leading_type_object_union, **{"objId": objId, "max_path_length": max_path_length})
        logging.info("end query for %s", objId)
        context = [objId]
        types_seen_distance = {}
        distance_seen = 0

        for record in result:
            if record['entType'] not in types_seen_distance:
                types_seen_distance[record['entType']] = record['distance']
                context.append(record['ent2Id'])
            else:
                if record['distance'] <= types_seen_distance[record['entType']]:
                    context.append(record['ent2Id'])

            if distance_seen < record['distance']:
                distance_seen = record['distance']
                if all([ot in types_seen_distance for ot in entity_types]):
                    break

        contexts4leading.append(context)

    compute_relation_index(contexts4leading, neo4j_connection, duckdb_conn, cidx, ot1, edges_db)


def compute_relation_index(contexts, neo4j_connection, duckdb_conn, cidx, context_name, edges):
    global incr_edge_idx
    edge2obj = []
    batch_size = 50000
    i = 0
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', dir="data/temp")
    temp_file.close()
    num_proc_execs = len(contexts)
    num_events = 0

    logging.info("start context query for %s", context_name)

    for pi_idx, context in enumerate(contexts):
        view = neo4j_connection.exec_query(get_process_instances_multiple_objects, **{"objectIdList": context})
        events = view[0]['eventList']
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
                #edges[edge] = str(incr_idx)
                edges[edge] = incr_edge_idx
                incr_edge_idx += 1
            edge2obj.append((int(edges[edge]), pi_idx))
            i += 1
    logging.info("end context query for %s", context_name)

    if len(edge2obj) > 0:
        with open(temp_file.name, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(edge2obj)

    if len(contexts) > 0:
        duckdb_conn.sql(f"COPY {context_name} FROM '{temp_file.name}' (DELIMITER ',')")
        duckdb_conn.commit()

    temp_file.close()
    os.remove(temp_file.name)

    duckdb_conn.execute("INSERT INTO viewmeta VALUES (?, ?, ?, ?, ?)",
                (cidx, context_name, num_proc_execs, num_events, num_events / num_proc_execs if num_proc_execs > 0 else 0))
    duckdb_conn.commit()

    logging.info("Ingested relation index")

def compute_leading_type_context(ot1, neo4j_connection):
    query_result = neo4j_connection.exec_query(get_leading_type_query, **{"ot1": ot1})
    contexts = []
    for record in query_result:
        #print(record)

        types_seen_distance = {}
        contextObjects = [record['id']]

        for neighbor in record['neighbors']:
            if neighbor['type'] not in types_seen_distance:
                types_seen_distance[neighbor['type']] = neighbor['distance']
                contextObjects.append(neighbor['id'])
            else:
                if neighbor['distance'] <= types_seen_distance[neighbor['type']]:
                    contextObjects.append(neighbor['id'])

        context = neo4j_connection.exec_query(get_process_instances_multiple_objects, **{"objectIdList": contextObjects})
        contexts.append(context)
    return contexts


#if __name__ == "__main__":
#    main()