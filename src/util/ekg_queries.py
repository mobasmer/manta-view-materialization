def get_entity_types_query():
    query_str = '''
               MATCH (e:Entity)
               RETURN DISTINCT e.EntityType
               '''

    return Query(query_str=query_str)


def get_object_pairs_query_var_k(ot1, ot2, k=1):
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

def get_object_pairs_query(ot1, ot2):
    query_str = f'''
                MATCH (ent1:Entity)-[:REL*1..6]-(ent2:Entity)
                WHERE ent1.EntityType = "$type1" AND ent2.EntityType = "$type2"
                RETURN DISTINCT ent1.uID as o1, ent2.uID as o2;
               '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "type1": ot1,
                     "type2": ot2
                 })


def get_events_for_objects_query(o1, o2):
    query_str = '''
                MATCH (e : Event)-[:CORR]->(ent : Entity)
                WHERE ent.uID = "$o1" OR ent.uID = "$o2"
                ORDER BY e.timestamp, elementId(e)
                WITH collect({id: elementId(e), timestamp: e.timestamp}) AS eventList
                RETURN eventList;
               '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "o1": o1,
                     "o2": o2
                 })

def get_contexts_query_object_pair(ot1, ot2):
    query_str = '''
                    MATCH (ent1 : Entity)-[:REL*1..6]-(ent2 : Entity)
                    WHERE ent1.EntityType = "$type1" AND ent2.EntityType = "$type2" AND ent1.uID < ent2.uID
                    WITH DISTINCT ent1.uID as uid1, ent2.uID as uid2
                    MATCH (e : Event)-[:CORR]->(ent:Entity)
                    WHERE ent.uID = uid1 OR ent.uID = uid2
                    WITH uid1, uid2, e
                    ORDER BY e.timestamp, id(e)
                    WITH uid1, uid2, collect({id: id(e), timestamp: e.timestamp}) AS eventList
                    RETURN {id1: uid1, id2: uid2} AS context, eventList;
                '''
    # '''
    #                     MATCH (ent1 : Entity)-[:REL*1..6]-(ent2 : Entity)
    #                     WHERE ent1.EntityType = "$type1" AND ent2.EntityType = "$type2" AND ent1.uID < ent2.uID
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

def get_contexts_query_single_object(ot1):
    query_str = '''
                    MATCH (ent : Entity)
                    WHERE ent.EntityType = "$type1"
                    MATCH (e : Event)-[:CORR]->(ent : Entity)
                    WITH ent, e 
                    ORDER BY e.timestamp, id(e)
                    WITH id(ent) as entID, collect({id: id(e), timestamp: e.timestamp}) AS eventList
                    RETURN {id1: entID} AS context, eventList;
                '''
    return Query(query_str=query_str,
                 template_string_parameters={
                     "type1": ot1
                 })
