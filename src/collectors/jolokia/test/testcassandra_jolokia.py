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

    # @patch.object(Collector, 'publish')
    # def test_should_work_with_real_data(self, publish_mock):
    #     def se(url):
    #         if url == 'http://localhost:8778/jolokia/list':
    #             return self.getFixture('listing')
    #         else:
    #             return self.getFixture('stats')
    #     patch_urlopen = patch('urllib2.urlopen', Mock(side_effect=se))

    #     patch_urlopen.start()
    #     self.collector.collect()
    #     patch_urlopen.stop()

    #     metrics = self.get_metrics()
    #     self.setDocExample(collector=self.collector.__class__.__name__,
    #                        metrics=metrics,
    #                        defaultpath=self.collector.config['path'])
    #     self.assertPublishedMany(publish_mock, metrics)

    def test_should_cmopute_percentiles_accurately(self):
        test_buckets_a = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,1,1,8,5,6,1,6,5,3,8,9,10,7,8,7,5,5,5,3,3,2,2,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
        percentile_value = self.collector.compute_percentile(collector.create_offsets(90), test_buckets_a, 50)
        self.assertEqual(percentile_value, 398.0)

    # TODO(kr)
    # regex for attributes

################################################################################
if __name__ == "__main__":
    unittest.main()
