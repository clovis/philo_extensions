#!/usr/bin/env python

import sys
import os
import gzip
import re
import cPickle
from subprocess import call
from operator import itemgetter



class Mallet(object):
    
    def __init__(self, db, mallet_path='', path='/var/lib/philologic/databases/'):
        self.mallet_exec = mallet_path + 'bin/mallet'
        self.db_path = path + db
    
    def import_dir(self):
        text_path = self.db_path + '/pruned_texts/'
        output = self.db_path + '/vectors.mallet'
        if path:
            call(self.mallet_exec + ' import-dir --keep-sequence --input ' + text_path + ' --output ' + output, shell=True)
        else:
            print 'Please provide a path.'
            sys.exit()
    
    def import_files(self):
        pass
    
    def train_topics(self, input_file=False, topics=0, threads=1):
        input_file = self.db_path + '/' + input_file
        output_path = self.db_path + '/topic_model/'
        if not os.path.isdir(output_path):
            os.makedirs(output_path, 0755)
        output_doc_topics = output_path + '/output_doc_topics'
        output_state = output_path + '/output_state.gz'
        if not input_file:
            print 'Please provide an input file.'
            sys.exit()
        elif not topics:
            print 'You need to set the number of topics.'
        command = self.mallet_exec + ' train-topics --input ' + input_file + ' --num-topics ' + str(topics) + ' --output-doc-topics '\
                + output_doc_topics + ' --output-state ' + output_state + ' --num-threads ' + str(threads) + ' --num-iterations 20000 --random-seed 1'
        call(command, shell=True) 

    def parse_topics(self, word_limit=100):
        output_file = self.db_path + '/topic_model/output_state.gz'
        topics = {}
        start = 0
        for line in gzip.open(output_file):
            if re.search('#beta', line):
                start = 1
                continue
            if not start:
                continue
            fields = line.split()
            word = fields[4]
            topic = fields[5]
            if topic not in topics:
                topics[topic] = {}
            if word not in topics[topic]:
                topics[topic][word] = 1
            else:
                topics[topic][word] += 1
        ordered_topics = {}
        for topic in topics:
            if topic not in ordered_topics:
                ordered_topics[topic] = []
            count = 0
            for word in sorted(topics[topic], key= topics[topic].get, reverse=True):
                if count > word_limit:
                    break
                ordered_topics[topic].append((word, topics[topic][word]))
                count += 1
            print ordered_topics[topic][:10]
        output = open(self.db_path + '/topic_model/topics.pickle', 'w')
        cPickle.dump(ordered_topics, output)
                
    def parse_topics_in_docs(self, limit=None, min_value=0):
        output_file = self.db_path + '/topic_model/output_doc_topics'
        path = re.compile('file:/' + self.db_path + '/pruned_texts/')
        extension = re.compile('\.txt')
        topic_prop = {}
        for line in open(output_file):
            if re.search('#', line):
                continue
            fields = line.split()[1:]
            doc = fields.pop(0)
            doc = path.sub('', doc)
            doc = extension.sub('', doc)
            topic_prop[doc] = []
            for pos, field in enumerate(fields):
                if self.isodd(pos):
                    continue
                proportion = float(fields[pos + 1])
                if proportion > min_value:
                    topic_prop[doc].append((field, proportion))
            topic_prop[doc] = sorted(topic_prop[doc], key=itemgetter(1), reverse=True)[:limit]        
        output = open(self.db_path + '/topic_model/topics_in_docs.pickle', 'w')
        cPickle.dump(topic_prop, output)
        
    def isodd(self, num):
        """Function taken from http://stackoverflow.com/questions/1089936/even-and-odd-number"""
        return num & 1 and True or False