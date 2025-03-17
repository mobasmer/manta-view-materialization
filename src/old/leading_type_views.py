import concurrent.futures
import logging

from networkx.classes import edges
from ocpa.objects.log.importer.csv import factory as csv_import_factory
from ocpa.objects.log.importer.ocel import factory as ocel_import_factory
from ocpa.algo.predictive_monitoring import tabular, sequential
from ocpa.algo.predictive_monitoring import factory as predictive_monitoring
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')


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

    # add connected component edges as well ?

    return edges_leading_types

'''
    Collects indices for each leading type to be used for later comparison.
    
    @return: list of tuples (index, relation_index, number of process executions) for each leading type
'''
def compute_indices_by_leading_type(filename, file_type="json", object_types=None, act_name=None, time_name=None, sep=None):
    relation_indices = []

    for i, obj_type in tqdm(enumerate(object_types), desc="Preparing relation indices for leading types"):
        tqdm.write(f"Start loading: {obj_type}")
        ocel = load_ocel_by_leading_type(filename, obj_type, file_type, object_types, act_name, time_name, sep)
        tqdm.write(f"Done loading: {obj_type}")

        tqdm.write(f"Start building relation index for {obj_type}")
        relation_index = get_relation_index(ocel)
        relation_indices.append((i, relation_index, len(ocel.process_executions)))
        tqdm.write(f"Finished building relation index for {obj_type}")

    # TODO: add connected component edges as well ?

    return relation_indices

#def process_object_type(i, obj_type, filename, file_type="json", object_types=None, act_name=None, time_name=None, sep=None):
def process_object_type(i, obj_type, filename, file_type="json", object_types=None, act_name=None, time_name=None, sep=None):
    logging.info(f"Start loading: {obj_type}")
    ocel = load_ocel_by_leading_type(filename, obj_type, file_type, object_types, act_name, time_name, sep)
    logging.info(f"Done loading: {obj_type}")

    logging.info(f"Start building relation index for {obj_type}")
    relation_index = get_relation_index(ocel)
    logging.info(f"Finished building relation index for {obj_type}")

    num_proc_exec = len(ocel.process_executions)
    num_of_events = sum([len(proc_exec) for proc_exec in ocel.process_executions])
    avg_num_of_events_per_trace = num_of_events / num_proc_exec if num_proc_exec > 0 else 0

    return {
        "view_idx": i,
        "relation_index": relation_index,
        "num_proc_exec": num_proc_exec,
        "num_of_events": num_of_events,
        "avg_num_of_events_per_trace": avg_num_of_events_per_trace
    }

def get_relation_index(ocel):
    relation_index = dict()
    for j, proc_exec in enumerate(ocel.process_executions):
        proc_exec_graph = ocel.get_process_execution_graph(j)
        for edge in proc_exec_graph.edges:
            if edge in relation_index:
                relation_index[edge].append(j)
            else:
                relation_index[edge] = [j]
    return relation_index

def compute_indices_by_leading_type_parallel(filename, file_type="json", object_types=None, act_name=None, time_name=None, sep=None):
    relation_indices = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_object_type, i, obj_type, filename) for i, obj_type in enumerate(object_types)]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures),
                           desc="Collecting relation indices"):
            relation_indices.append(future.result())

    return relation_indices

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