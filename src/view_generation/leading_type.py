from networkx.classes import edges
from ocpa.objects.log.importer.csv import factory as ocel_import_factory

def get_ocel(filename, leading_type, object_types, act_name, time_name, sep):
    parameters = {
        "obj_names": object_types,
        "val_names": [],
        "act_name": act_name,
        "time_name": time_name,
        "sep": sep,
        "execution_extraction": "leading_type",
        "leading_type": leading_type
    }
    ocel = ocel_import_factory.apply(file_path=filename, parameters=parameters)
    return ocel


def compute_edges_by_leading_type(filename, object_types, act_name, time_name, sep):
    edges_leading_types = []

    for i, obj_type in enumerate(object_types):
        ocel = get_ocel(filename, obj_type, object_types, act_name, time_name, sep)
        relation = set()

        for j, proc_exec in enumerate(ocel.process_executions):
            proc_exec_graph = ocel.get_process_execution_graph(j)
            relation.update(proc_exec_graph.edges)
        edges_leading_types.append((i, relation))

    return edges_leading_types