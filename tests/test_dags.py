import unittest
from airflow.models import DagBag

class TestDags(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(dag_folder='dags', include_examples=False)

    def test_dags_load_with_no_errors(self):
        assert len(self.dagbag.import_errors) == 0, f"DAG import errors: {self.dagbag.import_errors}"

    def test_scrape_rental_houses_dag_structure(self):
        dag = self.dagbag.get_dag('scrape_rental_houses')
        assert dag is not None
        assert len(dag.tasks) == 2
        assert dag.has_task('scrape_to_gcs')
        assert dag.has_task('gcs_to_bigquery')

if __name__ == '__main__':
    unittest.main()
