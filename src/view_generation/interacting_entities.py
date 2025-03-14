import csv
import itertools
import logging
import os
import pickle
import tempfile

import duckdb
from promg import DatabaseConnection, Query
from src.util.query_result_parser import parse_to_list, parse_to_2d_list
from src.view_generation.leading_type_views_db import compute_relation_index

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def get_entity_types_query():
    query_str = '''
               MATCH (e:Entity)
               RETURN DISTINCT e.EntityType
               '''

    return Query(query_str=query_str)


def get_object_pairs_query(ot1, ot2, k=1):
    match_string = ""
    if k > 1:
        for i in range(k-1):
            match_string += "-[:REL]-(:Entity)"
    query_str = f'''
               MATCH (e1:Entity){match_string}-[:REL]-(e2:Entity)
               WHERE e1.EntityType = "$type1" AND e2.EntityType = "$type2"
               RETURN DISTINCT e1.ID, e2.ID;
               '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "type1": ot1,
                     "type2": ot2
                 })

def get_events_for_objects_query(o1, o2):
    query_str = f'''
               MATCH (e:Event)-[:CORR]-(ent:Entity)
               WHERE ent.ID = "$o1" OR ent.ID = "$o2"
               RETURN e.idx, e.timestamp
               ORDER BY e.timestamp ASC
               '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "o1": o1,
                     "o2": o2
                 })

def compute_indices_by_interacting_types(temp_db_name, short_name):
    neo4j_connection = DatabaseConnection(
        db_name="neo4j",
        uri="bolt://localhost:7687",
        user="neo4j",
        password="12345678")

    result = neo4j_connection.exec_query(get_entity_types_query)
    entity_types = parse_to_list(result, 'e.EntityType')
    print(entity_types)
    context_defs = list(itertools.combinations(entity_types, 2))
    context_defs.extend([(t, t) for t in entity_types])
    print(context_defs)
    edges_leading_types = []

    edges = {}
    incr_idx = 0
    for i, (ot1, ot2) in enumerate(context_defs):
        relations = set()
        print("start object pair query", (ot1, ot2))
        result = neo4j_connection.exec_query(get_object_pairs_query, **{"ot1": ot1, "ot2": ot2})
        print("end object pair query", (ot1, ot2))
        object_pairs = parse_to_2d_list(result, 'e1.ID', 'e2.ID')
         # store object pairs for each object type pair s.t. we don't have to query them again
        with open(f"../../data/object_pairs/object_pairs_{ot1}_{ot2}.pkl", "wb") as f:
            pickle.dump(object_pairs, f)
        print(object_pairs)
        #with open(f"../../data/object_pairs/object_pairs_{ot1}_{ot2}.pkl", "rb") as f:
        #    object_pairs = pickle.load(f)
        con = duckdb.connect()

        compute_relation_index(neo4j_connection, ot1 + "+" + ot2, object_pairs, con, incr_idx, edges)
        edges_leading_types.append((i, relations))
        print("done with object pair", i)

def compute_relation_index(neo4j_connection, obj_type, object_pairs, con, incr_idx, edges):
    edge2obj = []
    batch_size = 50000
    i = 0
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    temp_file.close()

    for pi_idx, (o1, o2) in enumerate(object_pairs):
        #print(o1, o2)
        result = neo4j_connection.exec_query(get_events_for_objects_query, **{"o1": o1, "o2": o2})
        events = parse_to_2d_list(result, 'e.idx', 'e.timestamp')
        print(o1, o2, len(events))

        for j in range(len(events) - 1):
            if i == batch_size:
                with open(temp_file.name, 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerows(edge2obj)
                edge2obj = []
                i = 0
            edge = (events[j][0], events[j + 1][0])
            if edge not in edges:
                edges[edge] = incr_idx
                incr_idx += 1
            edge2obj.append((edges[edge], pi_idx))
            i += 1

    if len(edge2obj) > 0:
        with open(temp_file.name, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(edge2obj)

    con.sql(f"COPY {obj_type} FROM '{temp_file.name}' (DELIMITER ',')")
    os.remove(temp_file.name)

    logging.info("Ingested relation index")
