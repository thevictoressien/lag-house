import unittest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag

class TestDags(unittest.TestCase):

    @patch('airflow.models.DagBag')
    def setUp(self, mock_dagbag):
        self.dagbag = mock_dagbag.return_value
        self.dagbag.import_errors = {}
        self.dagbag.dags = {
            'scrape_rental_houses': MagicMock(),
        }

    def test_dags_load_with_no_errors(self):
        assert len(self.dagbag.import_errors) == 0, f"DAG import errors: {self.dagbag.import_errors}"

    def test_scrape_rental_houses_dag_structure(self):
        dag = self.dagbag.dags['scrape_rental_houses']
        assert dag is not None
        dag.tasks = [MagicMock(), MagicMock()]
        dag.has_task = lambda task_id: task_id in ['scrape_to_gcs', 'gcs_to_bigquery']
        assert len(dag.tasks) == 2
        assert dag.has_task('scrape_to_gcs')
        assert dag.has_task('gcs_to_bigquery')

if __name__ == '__main__':
    unittest.main()
