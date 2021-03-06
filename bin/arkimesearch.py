from elasticsearch import Elasticsearch
import os,sys
import time
import re
import shlex

from splunklib.searchcommands import dispatch, GeneratingCommand, Configuration, Option, validators
from splunk.clilib import cli_common as cli

@Configuration()
class ArkimeSearchCommand(GeneratingCommand):

    query = Option(require=False)

    # TODO: Proper configuration settings
    prefix = 'arkime'
    limit = 1000

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

        # TODO: A lot more error checking
        if self.query:
            self.query = self.query.replace('=', ' = ')
            querylist = shlex.split(self.query)
            
            record_filter = dict()
            record_filter['bool'] = dict()
            record_filter['bool']['must'] = list()
            
            for x in range(0, len(querylist), 3):
                values = querylist[x + 2].split(';')
                
                field_filter = dict()
                field_filter['bool'] = dict()
                field_filter['bool']['should'] = list()
            
                for value in values:
                    value_filter = dict()
                    value_filter['term'] = dict()
                    value_filter['term'][querylist[x]] = value
                    field_filter['bool']['should'].append(value_filter)
               
                record_filter['bool']['must'].append(field_filter)
            query['query']['bool']['filter'].append(record_filter)


        return query

    def read_config(self):

        cfg = cli.getMergedConf('arkime')

        self.elastic_nodes = dict()
        for stanza in cfg.keys():
            if re.match("^elastic:[^:]+$", stanza):
                elastic, site = stanza.split(':', 1)
                self.elastic_nodes[site] = list()
                for server in cfg[stanza]:
                    if re.match("^server\d+$", server):
                        self.elastic_nodes[site].append( cfg[stanza][server] )

    def generate(self):
        

        try:
            self.startTime = self.search_results_info.startTime
        except AttributeError:
            self.startTime = 0 

        try:
            self.endTime = self.search_results_info.endTime
        except AttributeError:
            self.endTime = time.time() 


        #self.elastic_nodes = self.read_config()
        self.read_config()

        # TODO: multithreading?
        for cluster in self.elastic_nodes:
          es = Elasticsearch(self.elastic_nodes[cluster])

          results = es.search(index="sessions*", body=self.search_query())

          for result in results['hits']['hits']:
              record = {}
              record = self.handle_results(result, self.prefix, record)

              record['host'] = cluster
              record['_time'] = self.timestamp(record['arkime._source.firstPacket'])
              record['_raw'] = str(record)

              yield record
 
dispatch(ArkimeSearchCommand, sys.argv, sys.stdin, sys.stdout, __name__)
