"""Test that all Airflow DAGs parse without errors."""
import os
import pytest


@pytest.mark.unit
class TestDagIntegrity:

    DAG_FOLDER = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "airflow", "dags"
    )

    def test_dag_folder_exists(self):
        assert os.path.isdir(self.DAG_FOLDER), f"DAG folder not found: {self.DAG_FOLDER}"

    def test_dag_files_exist(self):
        dag_files = [f for f in os.listdir(self.DAG_FOLDER) if f.endswith(".py")]
        assert len(dag_files) > 0, "No DAG files found"

    def test_dag_files_have_valid_syntax(self):
        """Check all DAG files compile without syntax errors."""
        dag_files = [f for f in os.listdir(self.DAG_FOLDER) if f.endswith(".py")]
        for dag_file in dag_files:
            filepath = os.path.join(self.DAG_FOLDER, dag_file)
            with open(filepath, "r") as f:
                source = f.read()
            try:
                compile(source, filepath, "exec")
            except SyntaxError as e:
                pytest.fail(f"Syntax error in {dag_file}: {e}")

    def test_all_dags_have_required_fields(self):
        """Check DAG files contain required DAG configuration."""
        dag_files = [f for f in os.listdir(self.DAG_FOLDER)
                     if f.endswith(".py") and f != "common.py"]
        for dag_file in dag_files:
            filepath = os.path.join(self.DAG_FOLDER, dag_file)
            with open(filepath, "r") as f:
                content = f.read()
            assert "dag_id" in content, f"{dag_file} missing dag_id"
            assert "start_date" in content, f"{dag_file} missing start_date"

    def test_no_hardcoded_credentials_in_dags(self):
        """Verify DAG files don't contain hardcoded credentials."""
        dag_files = [f for f in os.listdir(self.DAG_FOLDER) if f.endswith(".py")]
        forbidden = ["admin123456", "airflow123", "superset123"]
        for dag_file in dag_files:
            filepath = os.path.join(self.DAG_FOLDER, dag_file)
            with open(filepath, "r") as f:
                content = f.read()
            for secret in forbidden:
                assert secret not in content, \
                    f"{dag_file} contains hardcoded credential: {secret}"

    def test_expected_dags_present(self):
        """Check that all expected DAG files exist."""
        expected = [
            "weather_pipeline_dag.py",
            "weather_streaming_dag.py",
            "common.py",
        ]
        actual = os.listdir(self.DAG_FOLDER)
        for dag_file in expected:
            assert dag_file in actual, f"Expected DAG file missing: {dag_file}"
