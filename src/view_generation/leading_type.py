from networkx.classes import edges
from ocpa.objects.log.importer.csv import factory as csv_import_factory
from ocpa.objects.log.importer.ocel import factory as ocel_import_factory
from ocpa.algo.predictive_monitoring import tabular, sequential
from ocpa.algo.predictive_monitoring import factory as predictive_monitoring

def get_ocel_from_csv(filename, leading_type, object_types, act_name, time_name, sep):
    parameters = {
        "obj_names": object_types,
        "val_names": [],
        "act_name": act_name,
        "time_name": time_name,
        "sep": sep,
        "execution_extraction": "leading_type",
        "leading_type": leading_type
    }
    ocel = csv_import_factory.apply(file_path=filename, parameters=parameters)
    return ocel

def get_ocel_from_json(filename, leading_type):
    parameters = {
        "execution_extraction": "leading_type",
        "leading_type": leading_type
    }
    ocel = ocel_import_factory.apply(file_path=filename, parameters=parameters)
    return ocel

def compute_edges_by_leading_type(filename, file_type="json", object_types=None, act_name=None, time_name=None, sep=None):
    edges_leading_types = []

    for i, obj_type in enumerate(object_types):
        ocel = load_ocel_by_leading_type(filename, obj_type, file_type, object_types, act_name, time_name, sep)
        print("done loading", obj_type)
        relation = set()

        for j, proc_exec in enumerate(ocel.process_executions):
            proc_exec_graph = ocel.get_process_execution_graph(j)
            relation.update(proc_exec_graph.edges)
        edges_leading_types.append((i, relation))

    return edges_leading_types

def compute_edges_by_leading_type_sequence_encoding(filename, file_type="json", object_types=None, act_name=None, time_name=None, sep=None):
    edges_leading_types = []

    for i, obj_type in enumerate(object_types):
        ocel = load_ocel_by_leading_type(filename, obj_type, file_type, object_types, act_name, time_name, sep)
        relation = set()
        feature_storage = predictive_monitoring.apply(ocel, [], [])
        sequences = sequential.construct_sequence(feature_storage)
        print(sequences)

        for seq in enumerate(sequences):
            relation.update([(seq[i], seq[i+1]) for i in range(len(seq) - 1)])
        edges_leading_types.append((i, relation))

    return edges_leading_types

def load_ocel_by_leading_type(filename, obj_type, file_type="json", object_types=None, act_name=None, time_name=None, sep=None):
    if file_type == "json":
        ocel = get_ocel_from_json(filename, obj_type)
    else:
        ocel = get_ocel_from_csv(filename, obj_type, object_types, act_name, time_name, sep)
    return ocel