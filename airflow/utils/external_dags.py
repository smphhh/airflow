import importlib
import json
import os

def get_external_dag_paths(dag_dir_or_file_path):
    """
    Get paths to external DAG import files

    When given a path to a directory, returns paths to external DAG import files within this
    directory. When given a path to a file, returns the same path if the path points to a
    external DAG import file.
    """
    external_dag_config_path = _find_external_dag_config(dag_dir_or_file_path)
    if external_dag_config_path is not None:
        with open(external_dag_config_path, mode="rb") as f:
            external_dag_config = json.load(f)

        external_dag_provider_module = importlib.import_module(
            external_dag_config["external_dag_provider"])
        return external_dag_provider_module.get_external_dag_paths(
            os.path.dirname(external_dag_config_path), dag_dir_or_file_path, external_dag_config["config"])

    else:
        return []

def get_external_dag(dag_file_path):
    """
    Download all external DAG files for the DAG import file
    """
    if dag_file_path is None or os.path.exists(dag_file_path):
        return

    external_dag_config_path = _find_external_dag_config(dag_file_path)
    if external_dag_config_path is not None:
        with open(external_dag_config_path, mode="rb") as f:
            external_dag_config = json.load(f)

        external_dag_provider_module = importlib.import_module(
            external_dag_config["external_dag_provider"])
        external_dag_provider_module.get_external_dag(
            os.path.dirname(external_dag_config_path), dag_file_path, external_dag_config["config"])

        return

def _find_external_dag_config(path):
    for dag_dir_path in _yield_path_prefixes(path):
        external_dag_config_path = os.path.join(dag_dir_path, "external_dag_config.json")
        if os.path.exists(external_dag_config_path):
            return external_dag_config_path
    return None

def _yield_path_prefixes(path):
    path = os.path.normpath(path)
    while len(path) > 1:
        yield path
        path, _ = os.path.split(path)
