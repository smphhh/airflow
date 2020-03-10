import importlib

from airflow.configuration import conf

def get_external_dag_paths(dag_dir_or_file_path):
    """
    Get paths to external DAG import files

    When given a path to a directory, returns paths to external DAG import files within this
    directory. When given a path to a file, returns the same path if the path points to a
    external DAG import file.
    """
    external_dag_provider = _get_external_dag_provider()
    if external_dag_provider is not None:
        return external_dag_provider.get_external_dag_paths(dag_dir_or_file_path)
    else:
        return []

def sync_external_dag(dag_file_path):
    """
    Sync external DAG files
    """
    if dag_file_path is None:
        return
    
    external_dag_provider = _get_external_dag_provider()
    if external_dag_provider is not None:
        external_dag_provider.sync_external_dag(dag_file_path)

def _get_external_dag_provider():
    if conf.has_option("external_dags", "external_dag_provider"):
        return importlib.import_module(conf.get("external_dags", "external_dag_provider"))
    else:
        return None
