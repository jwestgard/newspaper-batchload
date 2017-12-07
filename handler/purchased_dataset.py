''' A handler for loading purchased datasets using CSV metadata. '''

import csv
import logging
import os
import sys
import yaml
from rdflib import Graph, Literal, Namespace, URIRef
from classes import pcdm
from classes.exceptions import ConfigException, DataReadException
from namespaces import bibo, dc, dcmitype, dcterms, pcdmuse, rdf

#============================================================================
# DATA LOADING FUNCTION
#============================================================================

def load(repo, batch_config):
    return Batch(repo, batch_config)


#============================================================================
# BATCH CLASS (FOR PURCHASED DATASETS)
#============================================================================

class Batch():

    '''Iterator class representing a set of resources to be loaded'''

    def __init__(self, repo, config):
        self.logger = logging.getLogger(
            __name__ + '.' + self.__class__.__name__
            )
        # Check for required configuration items and set up paths
        required_keys = ['HANDLER',
                         'COLLECTION',
                         'ROOT',
                         'MAPFILE',
                         'LOG_LOCATION',
                         'LOG_CONFIG',
                         'METADATA_FILE',
                         'DATA_PATH',
                         ] 
        for key in required_keys:
            if not config.get(key):
                raise ConfigException(
                    'Missing required key {0} in batch config'.format(key)
                    )
        self.items         = []
        self.local_path    = os.path.normpath(config.get('ROOT'))
        self.data_path     = os.path.join(self.local_path, config['DATA_PATH'])
        self.metadata_file = os.path.join(self.local_path,
                                          config.get('METADATA_FILE'))
        self.collection    = pcdm.Collection.from_repository(repo,
                                                     config.get('COLLECTION'))

        # Check for required metadata file
        if not os.path.isfile(self.metadata_file):
            raise ConfigException('Specified metadata file could not be found')

        # Generate item-level metadata graphs and store as files
        with open(self.metadata_file, 'r') as f:
            reader = csv.DictReader(f)
            for n, row in enumerate(reader):
                for k,v in row.items():
                    print("{0} => {1}".format(k,v))
                row['path'] = "{0}, row {1}".format(self.metadata_file, n+1)
                row['parts'] = []
                files = row['files'].split(';')
                row['files'] = [os.path.join(self.data_path, f) for f in files]
                self.items.append(row)

        # Create list of complete item keys and set up counters
        self.length = len(self.items)
        self.count = 0
        self.logger.info("Batch contains {0} items.".format(self.length))

    def __iter__(self):
        return self

    def __next__(self):
        if self.count < self.length:
            dataset = Dataset(self.items[self.count])
            dataset.add_collection(self.collection)
            self.count += 1
            return dataset
        else:
            self.logger.info('Processing complete!')
            raise StopIteration()


#============================================================================
# DATASET (ITEM) CLASS
#============================================================================

class Dataset(pcdm.Item):

    '''Class representing a purchased dataset'''

    def __init__(self, data):
        super().__init__()
        self.id            = id
        self.path          = data['path']
        self.title         = data['title']
        self.publisher     = data['publisher']
        self.filepaths     = data['files']
        self.sequence_attr = ('','')

    def read_data(self):
        for path in self.filepaths:
            self.add_file(File.from_localpath(path))

    def graph(self):
        graph = super(Dataset, self).graph()
        return graph


#============================================================================
# FILE CLASS
#============================================================================

class File(pcdm.File):

    '''Class representing file associated with a letter or page resource'''

    def graph(self):
        graph = super(File, self).graph()
        graph.add((self.uri, dcterms.title, Literal(self.title)))
        graph.add((self.uri, dcterms.type, dcmitype.Text))
        return graph
