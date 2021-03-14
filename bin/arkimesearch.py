from elasticsearch import Elasticsearch
import os,sys
import time

from splunklib.searchcommands import dispatch, GeneratingCommand, Configuration, Option, validators
from splunklib.six.moves import range

@Configuration()
class ArkimeSearchCommand(GeneratingCommand):

    prefix = 'arkime'

    def handle_results(self, result, prefix, record):
        if isinstance(result, dict):
            for key, value in result.items():
                if isinstance(value, dict):
                    record = self.handle_results(value, prefix + '.' + key, record)
                elif isinstance(value, list):
                    record[prefix + '.' + key] = list()
                    for item in value:
                      record[prefix + '.' + key].append(item)
                else:
                    record[prefix + '.' + key] = value
        return record
     
    def timestamp(self, time):
        seconds = int(time) / 1000
        milliseconds = int(time) % 1000
        return seconds + milliseconds / 1000

    def generate(self):
        es = Elasticsearch(['http://elastic1-1:9200'])

        results = es.search(index="sessions*")

        for result in results['hits']['hits']:
            record = {}
            record = self.handle_results(result, self.prefix, record)

            record['_time'] = self.timestamp(record['arkime._source.firstPacket'])
            record['_raw'] = str(record)

            yield record
 
dispatch(ArkimeSearchCommand, sys.argv, sys.stdin, sys.stdout, __name__)
