import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.platform_utils import notify_failure, notify_success

from enterprise_libs.workflow_factory import WorkflowDAGFactory, DatabricksTaskFactory

pipeline_id = "FINANCE_LEDGER_PIPELINE"

# pipeline specific vars
config_vars = Variable.get("platform-common-config", deserialize_json=True)
config_vars.update(Variable.get(f"platform-{pipeline_id.lower()}", deserialize_json=True))
env_config = config_vars

airflow_env = Variable.get("deployment_namespace")

if airflow_env == "airflow-dev":
    environments = ["DEV_CI"]
    deploy_target = "DEV"

if airflow_env == "airflow-stage":
    environments = ["STAGE"]
    deploy_target = "STAGE"

if airflow_env == "airflow-prod":
    environments = ["PRODUCTION"]
    deploy_target = "PROD"

for environment in environments:

    dag_builder = WorkflowDAGFactory(pipeline_id=pipeline_id, env=environment, vars=env_config)
    dag = dag_builder.build_dag()

    # ---------------- RAW INGEST ----------------

    raw_env = {
        **env_config,
        "DATABRICKS_DEPLOYMENT_NAME": f"FIN_LEDGER_RAW_{deploy_target}"
    }

    ingest_task_list = DatabricksTaskFactory(
        dag=dag,
        env=environment,
        pipeline_id=pipeline_id,
        process="ledger_raw_ingest",
        super_package="finance_raw",
        package="ledger_raw_pipeline",
        env_vars=raw_env
    )

    ingest_task_dict = ingest_task_list.get_ops()

    # ---------------- CLEANING ----------------

    clean_env = {
        **env_config,
        "DATABRICKS_DEPLOYMENT_NAME": f"FIN_LEDGER_CLEAN_{deploy_target}"
    }

    cleaning_task_list = DatabricksTaskFactory(
        dag=dag,
        env=environment,
        pipeline_id=pipeline_id,
        process="ledger_cleaning",
        super_package="finance_clean",
        package="ledger_clean_pipeline",
        previous_task_ids=[ingest_task_dict["run_op"].task_id],
        env_vars=clean_env
    )

    cleaning_task_dict = cleaning_task_list.get_ops()

    ingest_task_dict["run_op"] >> cleaning_task_dict["branch_op"]
    ingest_task_dict["skipped_op"] >> cleaning_task_dict["branch_op"]

    # ---------------- AGGREGATION ----------------

    agg_env = {
        **env_config,
        "DATABRICKS_DEPLOYMENT_NAME": f"FIN_LEDGER_AGG_{deploy_target}"
    }

    aggregation_task_list = DatabricksTaskFactory(
        dag=dag,
        env=environment,
        pipeline_id=pipeline_id,
        process="ledger_aggregation",
        super_package="finance_agg",
        package="ledger_aggregation_pipeline",
        previous_task_ids=[cleaning_task_dict["run_op"].task_id],
        env_vars=agg_env
    )

    aggregation_task_dict = aggregation_task_list.get_ops()

    cleaning_task_dict["run_op"] >> aggregation_task_dict["branch_op"]
    cleaning_task_dict["skipped_op"] >> aggregation_task_dict["branch_op"]

    # ---------------- REPORTING ----------------

    report_env = {
        **env_config,
        "DATABRICKS_DEPLOYMENT_NAME": f"FIN_LEDGER_REPORT_{deploy_target}"
    }

    reporting_task_list = DatabricksTaskFactory(
        dag=dag,
        env=environment,
        pipeline_id=pipeline_id,
        process="ledger_reporting",
        super_package="finance_reporting",
        package="ledger_report_pipeline",
        previous_task_ids=[aggregation_task_dict["run_op"].task_id],
        env_vars=report_env
    )

    reporting_task_dict = reporting_task_list.get_ops()

    aggregation_task_dict["run_op"] >> reporting_task_dict["branch_op"]
    aggregation_task_dict["skipped_op"] >> reporting_task_dict["branch_op"]
