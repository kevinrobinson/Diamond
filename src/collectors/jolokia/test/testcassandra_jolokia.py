#!/usr/bin/python
# coding=utf-8
################################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from mock import Mock
from mock import patch

from diamond.collector import Collector

from jolokia import CassandraJolokiaCollector

################################################################################


class TestCassandraJolokiaCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('CassandraJolokiaCollector', {})

        self.collector = CassandraJolokiaCollector(config, None)

    def test_import(self):
        self.assertTrue(CassandraJolokiaCollector)

    # TODO(kr)
    # collector = CassandraJolokiaCollector()
    # test_buckets_a = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,1,1,8,5,6,1,6,5,3,8,9,10,7,8,7,5,5,5,3,3,2,2,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    # print collector.compute_percentile(collector.create_offsets(90), test_buckets_a, 50)
    # should be: 398.0

    # TODO(kr)
    # regex for attributes

################################################################################
if __name__ == "__main__":
    unittest.main()
