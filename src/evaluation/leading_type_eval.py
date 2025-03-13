import datetime
import json
import time

from ocpa.algo.predictive_monitoring.execution_based_features.extraction_functions import number_of_events

from src.strategies.subset_selection import SubsetSelector
from src.view_generation.leading_type import compute_edges_by_leading_type, \
    compute_edges_by_leading_type_sequence_encoding, load_ocel_by_leading_type


def main():
    compute_views_for_bpi14_dataset()
    #compute_views_for_ocel_dataset()
    #compute_views_for_ocel_dataset_sequence()

# check that event ids are taken from the event log / assigned deterministically
def compute_views_for_bpi17():
    filename = '../../data/bpi17/BPI2017-Final-adapt.csv'
    object_types = ["offer", "application", "event_org:resource"]
    act_name = "event_activity"
    time_name = "event_timestamp"
    sep = ","

    edges_leading_types = compute_edges_by_leading_type(filename, object_types, act_name, time_name, sep)

    subset_selection = SubsetSelector(views=edges_leading_types, weight=0.5)
    selected_views = subset_selection.select_view_indices(3)

    print(selected_views)
    return selected_views

def compute_views_for_bpi14_dataset():
    filename = '../../data/bpi14/BPIC14.jsonocel'
    object_types = ["ConfigurationItem",
      "ServiceComponent",
      "Incident",
      "Interaction",
      "Change",
      "Case_R",
      "KM"]

    start_time = time.time()
    edges_leading_types = compute_edges_by_leading_type(filename, file_type="json", object_types=object_types)
    print("done with loading everything")
    subset_selection = SubsetSelector(views=edges_leading_types, weight=0.5)
    selected_views = subset_selection.select_view_indices(2)
    print(selected_views)

    get_stats_for_views(filename, selected_views,  object_types, edges_leading_types, start_time, "leading-type", "Order", file_type="json")
    return selected_views


def compute_views_for_ocel_dataset():
    filename = '../../data/order_ocel2/running-example.jsonocel'
    object_types = ["orders", "items", "packages", "customers", "products", "employees"]

    start_time = time.time()
    edges_leading_types = compute_edges_by_leading_type(filename, file_type="json", object_types=object_types)

    subset_selection = SubsetSelector(views=edges_leading_types, weight=0.5)
    selected_views = subset_selection.select_view_indices(2)

    get_stats_for_views(filename, selected_views,  object_types, edges_leading_types, start_time, "leading-type", "Order", file_type="json")

    print(selected_views)
    return selected_views

def compute_views_for_ocel_dataset_sequence():
    filename = '../../data/order_ocel2/running-example.jsonocel'
    object_types = ["orders", "items", "packages", "customers", "products", "employees"]


    start_time = time.time()
    edges_leading_types = compute_edges_by_leading_type_sequence_encoding(filename, file_type="json", object_types=object_types)

    subset_selection = SubsetSelector(views=edges_leading_types, weight=0.5)
    selected_views = subset_selection.select_view_indices(2)

    get_stats_for_views(filename, selected_views,  object_types, edges_leading_types, start_time, "leading-type-sequence", "Order", file_type="json")

    print(selected_views)
    return selected_views

def get_stats_for_views(filename, selected_views, object_types, edges_leading_types, start_time, method, short_name, file_type="json"):
    path = "../../results"
    result_json = {}
    result_json["filename"] = filename
    result_json["method"] = method
    result_json["selected_views"] = []
    for k, res_tuple in enumerate(selected_views):
        obj_idx, score, max_sim_to_prev, time = res_tuple
        results_for_k = {}
        obj_t = object_types[obj_idx]
        ocel = load_ocel_by_leading_type(filename, obj_t, file_type=file_type)

        #interestingness measures by murrilas et al. :
        #  number of traces present in an event log
        # Level of detail (LoD) (Eq. 2): average number of unique activities per trace
        # Average number of events (AE) (Eq. 3): average number of events per trace:

        results_for_k["object_type"] = obj_t
        results_for_k["num_process_executions"] = len(ocel.process_executions)
        #results_for_k["num_variants"] = len(ocel.variants())
        results_for_k["num_edges"] = len(edges_leading_types[obj_idx][1])
        results_for_k["score"] = score
        results_for_k["time"] = time - start_time
        results_for_k["position"] = k
        results_for_k["max_sim_to_prev"] = max_sim_to_prev

        number_of_events = sum([len(proc_exec) for proc_exec in ocel.process_executions])
        results_for_k["avg_num_of_events_per_trace"] = number_of_events/len(ocel.process_executions)
        result_json["selected_views"].append(results_for_k)

    file_id = str(datetime.datetime.now)

    with open(f"{path}/{file_id}_results_{short_name}_{method}.json", "w") as f:
        json.dump(result_json, f)

# compute selected views and gather statistics: number of process executions, number of variants,
# number of events covered, etc.
# check how difference in weight affects the selected views
# check how difference between selected views changes with increasing k -> convergence?

if __name__ == "__main__":
    main()
