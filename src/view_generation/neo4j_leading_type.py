from promg import DatabaseConnection

from src.util.ekg_queries import  \
        get_leading_type_query, get_process_instances_multiple_objects, get_objects_for_leading_type, \
        get_leading_objects_for_leading_type_object_iteratively

def main():
        neo4j_connection = DatabaseConnection(
                db_name="neo4j",
                uri="bolt://localhost:7687",
                user="neo4j",
                password="12341234")

        result = compute_leading_type_contexts_iteratively("ConfigurationItem", neo4j_connection, max_path_length=1000, entity_types=['ConfigurationItem', 'ServiceComponent', 'Incident', 'Interaction', 'Change', 'Case_R', 'KM'])
        print(result)

def compute_leading_type_contexts(ot1, neo4j_connection):
    query_result = neo4j_connection.exec_query(get_leading_type_query, **{"ot1": ot1})
    contexts = []
    for record in query_result:
        print(record)

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

def compute_leading_type_contexts_iteratively(ot1, neo4j_connection, max_path_length=1000, entity_types=None):
    query_results = neo4j_connection.exec_query(get_objects_for_leading_type, **{"ot1": ot1})
    contexts4objects = []
    for record in query_results:
        #print(record)
        objId = record['id']
        context = [objId]
        types_seen_distance = {}

        for i in range(max_path_length):
            result = neo4j_connection.exec_query(get_leading_objects_for_leading_type_object_iteratively, **{"objId": objId, "k": i})
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

        #print(context)
        contexts4objects.append(context)

    for context in contexts4objects:
        view = neo4j_connection.exec_query(get_process_instances_multiple_objects, **{"objectIdList": context})

if __name__ == "__main__":
    main()