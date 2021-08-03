import unittest
import warnings, os

from deephaven import Session
from tests.gen_test_data import make_random_csv


class BaseTestCase(unittest.TestCase):
    csv_file = 'test_csv_file'

    @classmethod
    def setUpClass(cls) -> None:
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        if not os.path.exists(BaseTestCase.csv_file):
            with open(BaseTestCase.csv_file, 'w'):
                pass
        make_random_csv(5, 1000, output_file=BaseTestCase.csv_file)

    @classmethod
    def tearDownClass(cls) -> None:
        if os.path.exists(BaseTestCase.csv_file):
            os.remove(BaseTestCase.csv_file)

    def setUp(self) -> None:
        self.session = Session()

    def tearDown(self) -> None:
        self.session.close()


if __name__ == '__main__':
    unittest.main()
