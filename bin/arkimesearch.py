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
     

    def generate(self):
        es = Elasticsearch(['http://elastic1-1:9200'])

        results = es.search(index="sessions*")

        for result in results['hits']['hits']:
            record = {}
            record['arkime_index'] = result['_index']
            record['_raw'] = 'Arkmime Record in index %s' % result['_index']
            record = self.handle_results(result, self.prefix, record)
            yield record
 
dispatch(ArkimeSearchCommand, sys.argv, sys.stdin, sys.stdout, __name__)
