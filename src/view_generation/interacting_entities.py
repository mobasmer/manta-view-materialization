import itertools
import pickle
from promg import DatabaseConnection, Query
from src.util.query_result_parser import parse_to_list, parse_to_2d_list


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


db_connection = DatabaseConnection(
    db_name="neo4j",
    uri="bolt://localhost:7687",
    user="neo4j",
    #password: "bpic2017promg"
    password="12345678")

result = db_connection.exec_query(get_entity_types_query)
entity_types = parse_to_list(result, 'e.EntityType')
print(entity_types)
type_pairs = list(itertools.combinations(entity_types, 2))
type_pairs.extend([(t, t) for t in entity_types])
print(type_pairs)
edges_leading_types = []
for i, (ot1, ot2) in enumerate(type_pairs):
    relations = set()
    print("start object pair query", (ot1, ot2))
    result = db_connection.exec_query(get_object_pairs_query, **{"ot1": ot1, "ot2": ot2})
    print("end object pair query", (ot1, ot2))
    object_pairs = parse_to_2d_list(result, 'e1.ID', 'e2.ID')
     # store object pairs for each object type pair s.t. we don't have to query them again
    with open(f"../../data/object_pairs/object_pairs_{ot1}_{ot2}.pkl", "wb") as f:
        pickle.dump(object_pairs, f)
    print(object_pairs)
    #with open(f"../../data/object_pairs/object_pairs_{ot1}_{ot2}.pkl", "rb") as f:
    #    object_pairs = pickle.load(f)

    for o1, o2 in object_pairs:
        #print(o1, o2)
        result = db_connection.exec_query(get_events_for_objects_query, **{"o1": o1, "o2": o2})
        events = parse_to_2d_list(result, 'e.idx', 'e.timestamp')
        print(o1, o2, len(events))
        #events.sort(key=lambda x: x[1])
        #print(len(events))
        for j in range(len(events) - 1):
            relations.add((events[j][0], events[j + 1][0]))
    edges_leading_types.append((i, relations))
    print("done with object pair", i)
