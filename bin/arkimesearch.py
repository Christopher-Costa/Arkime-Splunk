from elasticsearch import Elasticsearch
import os,sys
import time

from splunklib.searchcommands import dispatch, GeneratingCommand, Configuration, Option, validators
from splunklib.six.moves import range

@Configuration()
class ArkimeSearchCommand(GeneratingCommand):

    # TODO: Proper configuration settings
    prefix = 'arkime'
    limit = 1000
    elastic_nodes = [['elastic1-1', 'elastic1-2'],
                     ['elastic2-1', 'elastic2-2']]

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
        return time / 1000

    def generate(self):
        # TODO: multithreading?
        for nodelist in self.elastic_nodes:
          es = Elasticsearch(nodelist, scheme="http", port="9200")

          results = es.search(index="sessions*", body={"from": 0, "size": self.limit})

          for result in results['hits']['hits']:
              record = {}
              record = self.handle_results(result, self.prefix, record)

              record['host'] = nodelist[0]
              record['_time'] = self.timestamp(record['arkime._source.firstPacket'])
              record['_raw'] = str(record)

              yield record
 
dispatch(ArkimeSearchCommand, sys.argv, sys.stdin, sys.stdout, __name__)
