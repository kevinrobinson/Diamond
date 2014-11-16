# coding=utf-8

"""
Collects Cassandra JMX metrics from the Jolokia Agent.  Extends the JolokiaCollector to
interpret Histogram beans with information about the distribution of request latencies.

#### Example Configuration
CassandraJolokiaCollector can be configured apply only to attributes that match a
regular expression, and to collect specific percentiles from the histogram statistics.  The
format is shown below with the default values.

CassandraJolokiaCollector.conf

```
    percentiles '50,95,99'
    attribute_regex '.*HistogramMicros$'
```
"""

from jolokia import JolokiaCollector
import math
import string
import re


class CassandraJolokiaCollector(JolokiaCollector):
    # override to allow setting which percentiles will be collected
    def get_default_config_help(self):
        config_help = super(CassandraJolokiaCollector, self).get_default_config_help()
        config_help.update({
            'percentiles': 'Comma separated list of percentiles to be collected (e.g., "50,95,99").',
            'attribute_regex': 'Filter to only process attributes that match this regex'
        })
        return config_help

    # override to allow setting which percentiles will be collected
    def get_default_config(self):
        config = super(CassandraJolokiaCollector, self).get_default_config()
        config.update({
            'percentiles': '50,95,99',
            'attribute_regex': '.*HistogramMicros$'
        })
        return config

    def __init__(self, config, handlers):
        super(CassandraJolokiaCollector, self).__init__(config, handlers)
        self.update_config(self.config)

    def update_config(self, config):
        if config.has_key('percentiles'):
            self.percentiles = map(int, string.split(config['percentiles'], ','))
        if config.has_key('attribute_regex'):
            self.attribute_regex = re.compile(config['attribute_regex'])

    # override: Interpret beans that match the `attribute_regex` as histograms, and collect
    # percentiles from them.
    def interpet_bean_with_list(self, prefix, obj):
        if not self.attribute_regex.match(prefix):
            return

        buckets = obj
        offsets = self.create_offsets(len(obj))
        for percentile in self.percentiles:
            percentile_value = self.compute_percentile(offsets, buckets, percentile)
            self.publish("%s.p%s" % (prefix, percentile), percentile_value)

    # Adapted from Cassandra docs: http://www.datastax.com/documentation/cassandra/2.0/cassandra/tools/toolsCFhisto.html
    # The index corresponds to the x-axis in a histogram.  It represents buckets of values, which are
    # a series of ranges. Each offset includes the range of values greater than the previous offset
    # and less than or equal to the current offset. The offsets start at 1 and each subsequent offset
    # is calculated by multiplying the previous offset by 1.2, rounding up, and removing duplicates. The
    # offsets can range from 1 to approximately 25 million, with less precision as the offsets get larger.
    def compute_percentile(self, offsets, buckets, percentile_int):
        non_zero_points = sum(buckets)
        middle_point_index = math.floor(non_zero_points * (percentile_int / float(100)))

        points_seen = 0
        for index, bucket in enumerate(buckets):
            points_seen += bucket
            if points_seen >= middle_point_index:
                return round((offsets[index] - offsets[index - 1]) / 2)

    # Returns a list of offsets for `n` buckets.
    def create_offsets(self, bucket_count):
        last_num = 1
        offsets = [last_num]

        for index in range(bucket_count):
            next_num = round(last_num * 1.2)
            if next_num == last_num:
                next_num += 1
            offsets.append(next_num)
            last_num = next_num

        return offsets