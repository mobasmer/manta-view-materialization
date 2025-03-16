from duckdb.duckdb import query
from promg import Query

entity_id_attr = "id"
entity_type_attr = "type"
event_time_attr = "time"

# for BPI14
# entity_id_attr = "uID"
# entity_type_attr = "EventType"
# event_time_attr = "timestamp"

'''
   Applied this query to Order dataset beforehand:
   MATCH ( n2:Entity )<-[:CORR]-( e : Event ) -[:CORR]-> ( n1:Entity ) 
   WHERE n1 <> n2
   WITH DISTINCT n1, n2
   MERGE (n1)-[:REL]-(n2) 
'''

def get_entity_types_query():
    query_str = f'''
               MATCH (e:Entity)
               RETURN DISTINCT e.{entity_type_attr}
               '''

    return Query(query_str=query_str)


def get_object_pairs_query_var_k(ot1, ot2, k=1):
    match_string = ""
    if k > 1:
        for i in range(k-1):
            match_string += "-[:REL]-(:Entity)"
    query_str = f'''
                MATCH (e1:Entity){match_string}-[:REL]-(e2:Entity)
                WHERE e1.{entity_type_attr} = "$type1" AND e2.{entity_type_attr} = "$type2"
                RETURN DISTINCT e1.{entity_id_attr}, e2.{entity_id_attr};
               '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "type1": ot1,
                     "type2": ot2
                 })

def get_object_pairs_query(ot1, ot2):
    query_str = f'''
                MATCH (ent1:Entity)-[:REL*1..6]-(ent2:Entity)
                WHERE ent1.{entity_type_attr} = "$type1" AND ent2.{entity_type_attr} = "$type2"
                RETURN DISTINCT ent1.{entity_id_attr} as o1, ent2.{entity_id_attr} as o2;
               '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "type1": ot1,
                     "type2": ot2
                 })


def get_events_for_objects_query(o1, o2):
    query_str = f'''
                MATCH (e : Event)-[:CORR]->(ent : Entity)
                WHERE ent.{entity_id_attr} = "$o1" OR ent.{entity_id_attr} = "$o2"
                ORDER BY e.{event_time_attr}, elementId(e)''' +\
               '''WITH collect({id: elementId(e), timestamp: e.'''+ event_time_attr  +''') AS eventList
                RETURN eventList;
               '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "o1": o1,
                     "o2": o2
                 })

def get_contexts_query_object_pair(ot1, ot2):
    query_str = f'''
                    MATCH (ent1 : Entity)-[:REL*1..6]-(ent2 : Entity)
                    WHERE ent1.{entity_type_attr} = "$type1" AND ent2.{entity_type_attr} = "$type2" AND ent1.{entity_id_attr} < ent2.{entity_id_attr}
                    WITH DISTINCT ent1.{entity_id_attr} as uid1, ent2.{entity_id_attr} as uid2
                    MATCH (e : Event)-[:CORR]->(ent:Entity)
                    WHERE ent.{entity_id_attr} = uid1 OR ent.{entity_id_attr} = uid2''' +\
                f''' WITH uid1, uid2, e
                    ORDER BY e.{event_time_attr}, id(e) ''' +\
                ''' WITH uid1, uid2, collect({id: id(e), timestamp: e.''' + event_time_attr +''') AS eventList
                    RETURN {id1: uid1, id2: uid2} AS context, eventList;
                '''
    # '''
    #                     MATCH (ent1 : Entity)-[:REL*1..6]-(ent2 : Entity)
    #                     WHERE ent1.{entity_type_attr} = "$type1" AND ent2.{entity_type_attr} = "$type2" AND ent1.{entity_id_attr} < ent2.{entity_id_attr}
    #                     WITH DISTINCT ent1, ent2
    #                     MATCH (e : Event)-[:CORR]->(ent:Entity)
    #                     WHERE ent = ent1 OR ent = ent2
    #                     WITH ent1, ent2, e
    #                     ORDER BY e.timestamp, id(e)
    #                     WITH id(ent1) as ent1ID, id(ent2) as ent2ID, collect({id: id(e), timestamp: e.timestamp}) AS eventList
    #                     RETURN {id1: ent1ID, id2: ent2ID} AS context, eventList;
    #                 '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "type1": ot1,
                     "type2": ot2
                 })

def get_process_instances_multiple_objects(objectIdList):
    objectIds = str(objectIdList)
    query_str = f'''
                    MATCH (e : Event)-[:CORR]->(ent : Entity)
                    WHERE ent.{entity_id_attr} IN $objectIds
                    WITH e
                    ORDER BY e.{event_time_attr} ASC, elementId(e)''' +\
                ''' WITH collect({id: elementId(e), timestamp: e.''' + event_time_attr +'''}) AS eventList
                    RETURN eventList;
                '''
    #print(query_str)
    return Query(query_str=query_str,
                 template_string_parameters={
                     "objectIds": objectIds
                 })

''' 
    Gets instances by leading types as proposed by (Adams, 2022)
    by collecting all event pairs that are connected by a direct follow relation
    and have a common entity in the set of entities that are related to a given leading type object
'''
def get_process_instances_multiple_objects_partial_order(objectIdList):
    objectIds = str(objectIdList)
    query_str = f'''
                    MATCH (e : Event)-[:CORR]->(ent : Entity)
                    WHERE ent.{entity_id_attr} IN $objectIds
                    WITH collect(e) as events
                    UNWIND nodes AS n
                    UNWIND nodes AS m
                    MATCH (n)-[:DF]->(m)
                    MATCH (n)-[:DF]->(commonEntity : Entity)<-[:DF]-(m)
                    WHERE n <> m AND commonEntity.{entity_id_attr} IN $objectIds''' + \
                ''' WITH collect({source: elementId(n), target: elementId(m)}) AS eventList
                    RETURN relationList;
                    '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "objectIds": objectIds
                 })

def get_contexts_query_single_object(ot1):
    query_str = f'''
                    MATCH (ent : Entity)
                    WHERE ent.{entity_type_attr} = "$type1"
                    MATCH (e : Event)-[:CORR]->(ent : Entity)
                    WITH ent, e 
                    ORDER BY e.{event_time_attr}, id(e) ''' +\
                ''' WITH id(ent) as entID, collect({id: id(e), timestamp: e.''' + event_time_attr +'''}) AS eventList
                    RETURN {id1: entID} AS context, eventList;
                '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "type1": ot1
                 })

def get_objects_for_leading_type(ot1):
    query_str = f'''
                    MATCH (ent : Entity)
                    WHERE ent.{entity_type_attr} = "$type"
                    RETURN ent.{entity_id_attr} as id;
                '''
    #print(query_str)
    return Query(query_str=query_str,
                 template_string_parameters={
                     "type": ot1
                 })

def get_objects_for_leading_type_object_iteratively(objId, path_length=6):
    match_string = ""
    if path_length > 1:
        for i in range(path_length - 1):
            match_string += "-[:REL]-(:Entity)"

    query_str = f'''
                    MATCH (ent : Entity)
                    WHERE ent.{entity_id_attr} = "$objId" 
                    WITH ent
                    MATCH (ent){match_string}-[:REL]-(ent2 : Entity) 
                    RETURN DISTINCT ent2.{entity_id_attr} as ent2Id, ent2.{entity_type_attr} as entType
                '''
    #print(query_str)
    return Query(query_str=query_str,
                 template_string_parameters={
                     "objId": objId
                 })

def get_objects_for_leading_type_object_union(objId, max_path_length=1):
    if max_path_length == 1:
        query_str = f''' MATCH (ent : Entity)
                            WHERE ent.{entity_id_attr} = "$objId" 
                            WITH ent 
                            MATCH (ent)-[:REL]-(ent2 : Entity) 
                            RETURN ent2.{entity_id_attr} as ent2Id, ent2.{entity_type_attr} as entType, 1 as distance'''
    else:
        query_str = f''' MATCH (ent : Entity)
                         WHERE ent.{entity_id_attr} = "$objId" 
                         WITH ent ''' +\
                     ''' CALL{ ''' +\
                    f''' MATCH (ent)-[:REL]-(ent2 : Entity) 
                         RETURN ent2.{entity_id_attr} as ent2Id, ent2.{entity_type_attr} as entType, 1 as distance'''

    if max_path_length > 1:
        for i in range(max_path_length-1):
            match_string = ''''''
            for j in range(i):
                match_string += '''-[:REL]-(:Entity)'''
            query_str += f'''
                        UNION ALL
                        MATCH (ent){match_string}-[:REL]-(ent2 : Entity) 
                        RETURN DISTINCT ent2.{entity_id_attr} as ent2Id, ent2.{entity_type_attr} as entType, {i+1} as distance
            '''
    query_str += '''}
                    RETURN ent2Id, entType, distance    
                    ORDER BY distance ASC'''
    #print(query_str)
    return Query(query_str=query_str,
                 template_string_parameters={
                     "objId": objId
                 })


def get_leading_type_query(ot1):
    query_str = f'''
                    MATCH (ent : Entity)
                    WHERE ent.{entity_type_attr} = "$type"
                    WITH ent 
                    MATCH p = (ent)-[:REL*1..20]-(ent2 : Entity)
                    WITH ent.{entity_id_attr} as entID, ent2.{entity_id_attr} as ent2ID, ent2.{entity_type_attr} as entType, length(p) as pathLength
                    ORDER BY pathLength ASC''' +\
                ''' WITH entID, collect({id: ent2ID, type: entType, distance: pathLength}) as neighbors
                    RETURN entID as id, neighbors;
                '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "type": ot1
                 })



