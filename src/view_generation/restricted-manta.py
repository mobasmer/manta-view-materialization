import csv

import pandas as pd
from ocpa.objects.log.importer.csv import factory as ocel_import_factory


df = pd.read_csv('../../data/BPI2017-Final-adapt.csv', sep=';', encoding='ISO-8859-1')

def wrap_columns_in_list(input_file, output_file, column_names):
    """
    Reads a CSV file, wraps each value in the specified column in a list, and saves it back as a new CSV file.
    """
    with open(input_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile, delimiter=';')
        fieldnames = reader.fieldnames

        for column_name in column_names:
            if column_name not in fieldnames:
                raise ValueError(f"Column '{column_name}' not found in CSV file.")

        rows = []
        for row in reader:
            for column_name in column_names:
                row[column_name] = [row[column_name]]  # Wrap value in list
                rows.append(row)

    with open(output_file, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

#filename = '../data/BPI2017-Final-adapt.csv'
filename = '../../data/Detail Change - wrapped.csv'
object_types = ["CI Name (aff)", "CI Type (aff)", "CI Subtype (aff)", "Service Component WBS (aff)", "Change ID", "Risk Assessment", "Emergency Change", "CAB-approval needed"]
#wrap_columns_in_list(filename, '../data/Detail Change - wrapped.csv', object_types)
#object_types = ["offer", "application", "event_org:resource"]
edges_leading_types = {obj_t: set() for obj_t in object_types}
scores = {obj_t: 0 for obj_t in object_types}
for object in object_types:
    parameters = {
                    "obj_names":object_types,
                    "val_names":[],
                    #"act_name":"event_activity",
                    #"time_name":"event_timestamp",
                    "act_name": "Change Type",
                    "time_name": "Actual End",
                    "sep":",",
                    "execution_extraction": "leading_type",
                    "leading_type": object
                }
    ocel = ocel_import_factory.apply(file_path= filename,parameters = parameters)
    edges = set()
    print(len(ocel.process_executions))
    for i, proc_exec in enumerate(ocel.process_executions):
        proc_exec_graph = ocel.get_process_execution_graph(i)
        edges.update(proc_exec_graph.edges)
    edges_leading_types[object] = edges

def jaccard_sim_edges(edges1, edges2):
    intersection = edges1.intersection(edges2)
    if len(intersection) == 0:
        return 0
    return len(intersection) / (len(edges1) + len(edges2) - len(intersection))

for obj_t, edges in edges_leading_types.items():
    for obj_t2, edges2 in edges_leading_types.items():
        if obj_t != obj_t2:
            sim = jaccard_sim_edges(edges, edges2)
            scores[obj_t] += sim

print(scores)

