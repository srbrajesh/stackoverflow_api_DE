import unittest

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from src.data_processor.stackoverflow_data_processor import StackOverflowDataProcessor

class StackOverflowDataProcessorTests(unittest.TestCase):
    def setUp(self):
        self.processor = StackOverflowDataProcessor()

    def test_fetch_stackoverflow_data(self):
        tag = 'python'
        from_date = '2021-01-01'
        data = self.processor.fetch_stackoverflow_data(tag, from_date)
        
        self.assertIsNotNone(data)
        self.assertIsInstance(data, dict)

    def test_get_top_trending_tags(self):
        data = {
            'items': [
                {'tags': ['python', 'django', 'sql']},
                {'tags': ['python', 'pandas', 'dataframe']},
                {'tags': ['python', 'numpy', 'array']},
                {'tags': ['java', 'spring', 'hibernate']},
                {'tags': ['javascript', 'react', 'frontend']}
            ]
        }
        count = 2
        trending_tags_df = self.processor.get_top_trending_tags(data, count)

        self.assertEqual(trending_tags_df.count(), count)

    def test_write_into_lake(self):
        data = {
            'items': [
                {'question_id': 1, 'title': 'Sample Question 1', 'tags': ['python']},
                {'question_id': 2, 'title': 'Sample Question 2', 'tags': ['java']},
                {'question_id': 3, 'title': 'Sample Question 3', 'tags': ['python', 'django']}
            ]
        }
        destination_path = '/tmp/stackoverflow_data'
        result_df = self.processor.write_into_lake(data, destination_path)

        self.assertIsNotNone(result_df)

if __name__ == '__main__':
    unittest.main()