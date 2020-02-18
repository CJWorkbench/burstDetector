import unittest
from burstDetector import migrate_params


class MigrateParamsTest(unittest.TestCase):
    def test_v0(self):
        self.assertEqual(migrate_params({
            'date_column_name': 'x',
            'trigger_threshold': 3,
            'interval_length': 3,
            'interval_unit': 2,
        }), {
            'date_column_name': 'x',
            'trigger_threshold': 3,
            'interval_length': 3,
            'interval_unit': 'hours',
        })

    def test_v1(self):
        self.assertEqual(migrate_params({
            'date_column_name': 'x',
            'trigger_threshold': 3,
            'interval_length': 3,
            'interval_unit': 'hours',
        }), {
            'date_column_name': 'x',
            'trigger_threshold': 3,
            'interval_length': 3,
            'interval_unit': 'hours',
        })


if __name__ == '__main__':
    unittest.main()
