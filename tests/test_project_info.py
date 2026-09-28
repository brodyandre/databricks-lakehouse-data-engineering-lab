from databricks_lakehouse.project_info import get_architecture_name, get_project_name


def test_project_name() -> None:
    assert get_project_name() == "databricks-lakehouse-data-engineering-lab"


def test_architecture_name() -> None:
    assert get_architecture_name() == "medallion"
