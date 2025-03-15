import concurrent.futures
import csv
import logging
import os

from ocpa.objects.log.importer.csv import factory as csv_import_factory
from ocpa.objects.log.importer.ocel import factory as ocel_import_factory
from tqdm import tqdm
import duckdb
import tempfile
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
def compute_indices_by_leading_type_db(filename, db_name, file_type="json", object_types=None, act_name=None,
                                       time_name=None, sep=None, duckdb_config=None):

    config = {}
    if duckdb_config is not None:
        if "memory_limit" in duckdb_config:
            config["memory_limit"] = duckdb_config["memory_limit"]
        if "threads" in duckdb_config:
            config["threads"] = duckdb_config["threads"]

    with duckdb.connect(db_name, config = config) as con:
        con.sql("DROP TABLE IF EXISTS viewmeta")
        con.sql("CREATE TABLE IF NOT EXISTS viewmeta(viewIdx INTEGER, objecttype STRING, numProcExecs INTEGER, numEvents INTEGER, AvgNumEventsPerTrace FLOAT)")

        con.sql("DROP TABLE IF EXISTS edges")
        con.sql("CREATE TABLE IF NOT EXISTS edges(source INTEGER, target INTEGER, edgeId INTEGER primary key)")

        for object_type in object_types:
            con.sql("DROP TABLE IF EXISTS " + object_type)
            con.sql("CREATE TABLE IF NOT EXISTS " + object_type + "(edge INTEGER, procExec INTEGER)")

        con.commit()

        incr_idx = 1
        edges = dict()

        for i, obj_type in tqdm(enumerate(object_types), desc="Preparing relation indices for leading types"):
            logging.info(f"Start loading: {obj_type}")
            ocel = load_ocel_by_leading_type(filename, obj_type, file_type, object_types, act_name, time_name, sep)
            logging.info(f"Done loading: {obj_type}")

            logging.info(f"Start building relation index for {obj_type}")
            compute_relation_index(obj_type, ocel, con, incr_idx, edges)

            num_proc_exec = len(ocel.process_executions)
            num_of_events = sum([len(proc_exec) for proc_exec in ocel.process_executions])
            avg_num_of_events_per_trace = num_of_events / num_proc_exec if num_proc_exec > 0 else 0
            con.execute("INSERT INTO viewmeta VALUES (?, ?, ?, ?, ?)",
                    (i, obj_type, num_proc_exec, num_of_events, avg_num_of_events_per_trace))
            con.commit()

            con.sql("CREATE INDEX IF NOT EXISTS " + obj_type + "_edge_index ON " + obj_type + "(edge)")
            con.commit()
            logging.info(f"Finished building relation index for {obj_type}")

def compute_relation_index(obj_type, ocel, con, incr_idx, edges):
    edge2obj = []
    batch_size = 50000
    i = 0
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    temp_file.close()
    logging.info("Started process executions")
    process_executions = ocel.process_executions
    logging.info("Computed process executions")
    for j, proc_exec in enumerate(process_executions):
        proc_exec_graph = ocel.get_process_execution_graph(j)
        for edge in proc_exec_graph.edges:
            if i == batch_size:
                with open(temp_file.name, 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerows(edge2obj)
                edge2obj = []
                i = 0
            if edge not in edges:
                edges[edge] = incr_idx
                incr_idx += 1
            edge2obj.append((edges[edge], j))
            i += 1

    if len(edge2obj) > 0:
        with open(temp_file.name, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(edge2obj)

    logging.info("Collected relation index")
    con.sql(f"COPY {obj_type} FROM '{temp_file.name}' (DELIMITER ',')")
    os.remove(temp_file.name)

    logging.info("Ingested relation index")

def compute_indices_by_leading_type_parallel_db(filename, file_type="json", object_types=None, act_name=None, time_name=None, sep=None):
    conn_name = "leading_type_views.duckdb"
    with duckdb.connect(conn_name) as con:
        con.sql("DROP TABLE IF EXISTS viewmeta")
        con.sql("CREATE TABLE IF NOT EXISTS viewmeta(viewIdx INTEGER, objecttype STRING, numProcExecs INTEGER, numEvents INTEGER, AvgNumEventsPerTrace FLOAT)")

        con.sql("DROP TABLE IF EXISTS edges")
        con.sql("CREATE TABLE IF NOT EXISTS edges(edgeId INTEGER primary key, source INTEGER, target INTEGER)")

        for object_type in object_types:
            con.sql("DROP TABLE IF EXISTS " + object_type)
            #con.sql("CREATE TABLE IF NOT EXISTS " + object_type + "(source INTEGER, target INTEGER, procExec INTEGER)")
            con.sql("CREATE TABLE IF NOT EXISTS " + object_type + "(edge INTEGER, procExec INTEGER)")

    #con = sqlite3.connect("leading_type_views.db")
    #cursor = con.cursor()
    #cursor.execute("DROP TABLE IF EXISTS viewmeta")
    #cursor.execute(
    #    "CREATE TABLE IF NOT EXISTS viewmeta(viewIdx, objecttype, numProcExecs, numEvents, AvgNumEventsPerTrace)")

    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(process_object_type, i, obj_type, filename, conn_name) for i, obj_type in enumerate(object_types)]
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures),
                           desc="Collecting relation indices"):
            future.result()

def process_object_type(i, obj_type, filename, conn_name, file_type="json", object_types=None, act_name=None, time_name=None, sep=None):
    #with sqlite3.connect(conn_name) as con:
    with duckdb.connect(conn_name, read_only=False) as con:
        print("I am here!")
        #cursor = con.cursor()
        #cursor.execute("CREATE TABLE IF NOT EXISTS " + obj_type + "(source INTEGER, target INTEGER, procExec INTEGER)")
        #con.sql("CREATE TABLE IF NOT EXISTS " + obj_type + "(source INTEGER, target INTEGER, procExec INTEGER)")

        tqdm.write(f"Start loading: {obj_type}")
        ocel = load_ocel_by_leading_type(filename, obj_type, file_type, object_types, act_name, time_name, sep)
        tqdm.write(f"Done loading: {obj_type}")

        tqdm.write(f"Start building relation index for {obj_type}")
        compute_relation_index(obj_type, ocel, con)
        con.commit()

        num_proc_exec = len(ocel.process_executions)
        num_of_events = sum([len(proc_exec) for proc_exec in ocel.process_executions])
        avg_num_of_events_per_trace = num_of_events / num_proc_exec if num_proc_exec > 0 else 0

        #cursor.execute("INSERT INTO viewmeta VALUES (?, ?, ?, ?, ?)", (i, obj_type, num_proc_exec, num_of_events, avg_num_of_events_per_trace))
        #cursor.execute(
        #    "CREATE INDEX IF NOT EXISTS " + obj_type + "_target_index ON " + obj_type + "(target)")
        #cursor.execute(
        #    "CREATE INDEX IF NOT EXISTS " + obj_type + "_source_index ON " + obj_type + "(source)")
        con.sql("INSERT INTO viewmeta VALUES (?, ?, ?, ?, ?)", (i, obj_type, num_proc_exec, num_of_events, avg_num_of_events_per_trace))
        con.sql("CREATE INDEX IF NOT EXISTS " + obj_type + "_target_index ON " + obj_type + "(target)")
        con.sql("CREATE INDEX IF NOT EXISTS " + obj_type + "_source_index ON " + obj_type + "(source)")
        con.commit()
        tqdm.write(f"Finished building relation index for {obj_type}")

def load_ocel_by_leading_type(filename, obj_type, file_type="json", object_types=None, act_name=None, time_name=None, sep=None):
    if file_type == "json":
        ocel = get_ocel_from_json(filename, obj_type)
    else:
        ocel = get_ocel_from_csv(filename, obj_type, object_types, act_name, time_name, sep)
    return ocel