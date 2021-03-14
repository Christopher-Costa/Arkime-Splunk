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

    def search_query(self):

        query = dict()
        query['from'] = 0
        query['size'] = self.limit 
        
        query['query'] = dict()
       
        query['query']['bool'] = dict()
        query['query']['bool']['filter'] = list()

        time_filter = dict()
        time_filter['range'] = dict()
        time_filter['range']['lastPacket'] = dict()
        time_filter['range']['lastPacket']['gte'] = self.startTime * 1000
        time_filter['range']['lastPacket']['lte'] = self.endTime * 1000

        query['query']['bool']['filter'].append(time_filter)

        return query

    def generate(self):

        try:
            self.startTime = self.search_results_info.startTime
        except AttributeError:
            self.startTime = 0 

        try:
            self.endTime = self.search_results_info.endTime
        except AttributeError:
            self.endTime = time.time() 


        # TODO: multithreading?
        for nodelist in self.elastic_nodes:
          es = Elasticsearch(nodelist, scheme="http", port="9200")

          results = es.search(index="sessions*", body=self.search_query())

          for result in results['hits']['hits']:
              record = {}
              record = self.handle_results(result, self.prefix, record)

              record['_time'] = self.timestamp(record['arkime._source.firstPacket'])
              record['_raw'] = str(record)

              yield record
 
dispatch(ArkimeSearchCommand, sys.argv, sys.stdin, sys.stdout, __name__)
