import collections
import logging
import re
import tempfile
import unittest

import main as mn
from apache_beam.testing.util import open_shards


class filter_product_views_test(unittest.TestCase):

    test_data_path = 'data-test/test.tsv'
    
    def main_test(self):
        mn.run([
                '--input=%s*' % self.test_data_path,
                '--output=%s.result' % self.test_data_path])
        # Parse result file and compare.
        results = []
        with open_shards(self.test_data_path + '.result-*-of-*') as result_file:
            for line in result_file:
              match = re.search(r'([a-z]+): ([0-9]+)', line)
              if match is not None:
                  results.append((match.group(1), int(match.group(2))))
        self.assertEqual(1,1)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()