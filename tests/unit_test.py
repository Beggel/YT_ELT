def test_api_key(api_key):
  assert api_key == "MOCK_KEY1234"

def test_channel_handle(channel_handle):
  assert channel_handle == "MRCHEES"

def test_postgres_conn(mock_postgres_conn_vars):
  conn = mock_postgres_conn_vars

  assert conn.login == "mock_username"
  assert conn.password == "mock_password"
  assert conn.host == "mock_host"
  assert conn.port == 1234
  assert conn.schema == "mock_db_name"

def test_dags_integrity(dagBag):
  # 1.
  print("====================")
  print(dagBag.import_errors)
  assert dagBag.import_errors == {}, f"Import errors found: {dagBag.import_errors}"
  
  # 2.
  expected_dag_ids = ["produce_json", "update_db", "data_quality"]
  loaded_dag_ids = list(dagBag.dags.keys())

  print("====================")
  print(dagBag.dags.keys())
  for dag_id in expected_dag_ids:
    assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing."
  
  # 3.
  print("====================")
  print(dagBag.size())
  assert dagBag.size() == 3
  
  # 4.
  expected_task_count = {
    "produce_json": 5,
    "update_db": 3,
    "data_quality": 2
  }
  print("====================")
  
  for dag_id, dag in dagBag.dags.items():
    expected_count = expected_task_count[dag_id]
    actual_count = len(dag.tasks)

    assert (
      expected_count == actual_count
    ), f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}."
    print(dag_id, len(dag.tasks))




