from pathlib import Path
import ast

def test_dag_has_six_tasks():
    path=Path(__file__).resolve().parents[1]/'airflow/dags/rapidgo_sla_orchestration.py'
    tree=ast.parse(path.read_text(encoding='utf-8'))
    names=[node for node in ast.walk(tree) if isinstance(node,ast.Call)
           and isinstance(node.func,ast.Name) and node.func.id=='PythonOperator']
    assert len(names)>=5

def test_dag_has_retries_and_no_schedule():
    code=(Path(__file__).resolve().parents[1]/'airflow/dags/rapidgo_sla_orchestration.py').read_text()
    assert "'retries': 2" in code
    assert 'schedule=None' in code
    assert 'xcom_pull' in (Path(__file__).resolve().parents[1]/'semana3/andres/orchestration.py').read_text()
