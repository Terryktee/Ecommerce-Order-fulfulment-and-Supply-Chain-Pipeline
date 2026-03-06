def test_dags_integrity(dagbag):
    assert dagbag.import_errors == {} , f"import errors found {dagbag.import_errors}"
    print("===================")
    print(dagbag.import_errors)

    expected_dag_ids = ["Bronze_Supply_Chain_Ingest" , "Silver_Supply_Chain_Transform" ,
                         "Gold_Supply_Chain_Star_Schema","Gold_Supply_Chain_Data_Quality_Check"]
    loaded_data_ids = list(dagbag.dags.keys())
    print("=================")
    print(dagbag.dags.keys())
 
    for dag_id in expected_dag_ids:
        assert dag_id in loaded_data_ids , f"DAG {dag_id} is missing."

    assert dagbag.size() == 4
    print("==============")
    print(dagbag.size())

    expected_task_counts = {
        "Bronze_Supply_Chain_Ingest":2,
        "Silver_Supply_Chain_Transform":5,
        "Gold_Supply_Chain_Star_Schema":10,
        "Gold_Supply_Chain_Data_Quality_Check":7,

    } 
    print("==========================")
    for dag_id,dag in dagbag.dags.items():
        expected_count = expected_task_counts[dag_id]
        actual_count = len(dag.tasks)

        assert(
            expected_count == actual_count
        ),f"DAG {dag_id} has actual_count tasks,expected {expected_count}."
        print(dag_id,len(dag.tasks))

