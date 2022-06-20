import yaml
import os
import json
import networkx as nx
import pickle

# README
# This file is a utility script that is run via CircleCI in the deploy
# step. It is not run via Airflow in any way. The point of this script is
# to generate a pickle file that contains all of the dependencies between dbt models
# for each DAG (usually corresponding to a different schedule) that we want
# to run.

def load_manifest():
   """Load manifest.json """
   local_filepath = f"{DBT_DIR}/target/manifest.json"
   with open(local_filepath) as f:
    data = json.load(f)
   return data

def load_model_selectors():
   """Load the dbt selectors from YAML file to be used with dbt ls command"""
   with open(f"{DBT_DIR}/selectors.yml") as f:
    dag_model_selectors = yaml.full_load(f)
   selected_models = {}
   for selector in dag_model_selectors["selectors"]:
    selector_name = selector["name"]
    selector_def = selector["definition"]
    selected_models[selector_name] = selector_def
   return selected_models

def parse_model_selector(selector_def):
   """Run the dbt ls command which returns all dbt models associated with a particular
   selection syntax"""
   models = os.popen(f"cd {DBT_DIR} && dbt ls --models {selector_def}").read()
   models = models.splitlines()
   return models

def generate_all_model_dependencies(all_models, manifest_data):
   """Generate dependencies for entire project by creating a list of tuples that
   represent the edges of the DAG"""
   dependency_list = []
   for node in all_models:
    # Cleaning things up to match node format in manifest.json
    split_node = node.split(".")
    length_split_node = len(split_node)
    node = split_node[0] + "." + split_node[length_split_node - 1]
    node = "model." + node
    node_test = node.replace("model", "test")
    # Set dependency to run tests on a model after model runs finishes
    dependency_list.append((node, node_test))
    # Set all model -> model dependencies
    for upstream_node in manifest_data["nodes"][node]["depends_on"]["nodes"]:
        upstream_node_type = upstream_node.split(".")[0]
        upstream_node_name = upstream_node.split(".")[2]
        if upstream_node_type == "model":
            dependency_list.append((upstream_node, node))
   return dependency_list

def clean_selected_task_nodes(selected_models):
   """Clean up the naming of the "selected" nodes so they match the structure of what
   is coming out of the generate_all_model_dependencies function. This function doesn't create
   a list of dependencies between selected nodes (that happens in generate_dag_dependencies), rather
   it's just cleaning up the naming of the nodes and outputting them as a list"""
   selected_nodes = []
   for node in selected_models:
    # Cleaning things up to match node format in manifest.json
    split_node = node.split(".")
    length_split_node = len(split_node)
    node = split_node[0] + "." + split_node[length_split_node - 1]
    # Adding run model nodes
    node = "model." + node
    selected_nodes.append(node)
    # Set test model nodes
    node_test = node.replace("model", "test")
    selected_nodes.append(node_test)
   return selected_nodes

def generate_dag_dependencies(selected_nodes, all_model_dependencies):
   """Return dependencies as list of tuples for a given DAG (set of models)"""
   G = nx.DiGraph()
   G.add_edges_from(all_model_dependencies)
   G_subset = G.copy()
   for node in G:
    if node not in selected_nodes:
        G_subset.remove_node(node)
   selected_dependencies = list(G_subset.edges())
   return selected_dependencies

def run():
   """Gets a list of all models in the project and creates dependencies.
   We want to load all the models first because the logic to properly set
   dependencies between subsets of models is basically
   removing nodes from the complete DAG. This logic can be found in the
   generate_dag_dependencies function. The networkx graph object is smart
   enough that if you remove nodes with remove_node method that the dependencies
   of the remaining nodes are what you would expect.
   """
   manifest_data = load_manifest()
   all_models = parse_model_selector("updater_data_model")
   all_model_dependencies = generate_all_model_dependencies(all_models, manifest_data)
   # Load model selectors
   dag_model_selectors = load_model_selectors()
   for dag_name, selector in dag_model_selectors.items():
    selected_models = parse_model_selector(selector)
    selected_nodes = clean_selected_task_nodes(selected_models)
    dag_dependencies = generate_dag_dependencies(selected_nodes, all_model_dependencies)
    with open(f"{DBT_DIR}/dbt_dags/data/{dag_name}.pickle", "wb") as f:
        pickle.dump(dag_dependencies, f)

# RUN IT
DBT_DIR = "./dags/dbt"
run()

with DAG(
    dag_id="dbt_dag",
    schedule_interval="@daily",
    max_active_runs=1,
    catchup=False,
    start_date=datetime(2021, 1, 1)
) as dag:
    # Load dependencies from configuration file
    dag_def = load_dag_def_pickle(f"{DAG_NAME}.pickle")

    # Returns a dictionary of bash operators corresponding to dbt models/tests
    dbt_tasks = create_task_dict(dag_def)

    # Set dependencies between tasks according to config file
    for edge in dag_def:
        dbt_tasks[edge[0]] >> dbt_tasks[edge[1]]
        
with dag:

    start_dummy = DummyOperator(task_id='start')
    
    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'dbt {DBT_GLOBAL_CLI_FLAGS} seed --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}'
    )
    end_dummy = DummyOperator(task_id='end')

    dag_parser = DbtDagParser(dag=dag,
                            dbt_global_cli_flags=DBT_GLOBAL_CLI_FLAGS,
                            dbt_project_dir=DBT_PROJECT_DIR,
                            dbt_profiles_dir=DBT_PROJECT_DIR,
                            dbt_target=DBT_TARGET
                            )
    dbt_run_group = dag_parser.get_dbt_run_group()
    dbt_test_group = dag_parser.get_dbt_test_group()

    start_dummy >> dbt_seed >> dbt_run_group >> dbt_test_group >> end_dummy