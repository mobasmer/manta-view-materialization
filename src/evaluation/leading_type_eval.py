from networkx.classes import edges
from ocpa.objects.log.importer.csv import factory as ocel_import_factory
from src.strategies.subset_selection import SubsetSelector



def main():
    compute_views_for_bpi17()

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


def compute_views_for_bpi17():
    filename = '../../data/BPI2017-Final-adapt.csv'
    object_types = ["offer", "application", "event_org:resource"]
    act_name = "event_activity"
    time_name = "event_timestamp"
    sep = ","

    edges_leading_types = compute_edges_by_leading_type(filename, object_types, act_name, time_name, sep)

    subset_selection = SubsetSelector(views=edges_leading_types, weight=0.5)
    selected_views = subset_selection.select_view_indices(2)

    print(selected_views)
    return selected_views


def compute_views_for_bpi14():
    # filename = '../../data/Detail Change - wrapped.csv'
    # object_types = ["CI Name (aff)", "CI Type (aff)", "CI Subtype (aff)", "Service Component WBS (aff)", "Change ID", "Risk Assessment", "Emergency Change", "CAB-approval needed"]
    pass


if __name__ == "__main__":
    main()
