#!/usr/bin/env python

from __future__ import division
import sys
import os
import gzip
import re
import sqlite3
import json
from subprocess import call
from operator import itemgetter
from numpy import zeros, float32, save


class Mallet(object):
    
    def __init__(self, db, mallet_path='', path='/var/lib/philologic/databases/', topics=10):
        self.mallet_exec = mallet_path + 'bin/mallet'
        self.db_path = path + db
        self.topics = topics
    
    def import_dir(self):
        text_path = self.db_path + '/pruned_texts/'
        output = self.db_path + '/vectors.mallet'
        call(self.mallet_exec + " import-dir --keep-sequence --token-regex '[\p{L}\p{M}]+' --input " + text_path + ' --output ' + output, shell=True)
    
    def import_files(self):
        pass
    
    def train_topics(self, threads=1):
        input_file = self.db_path + '/vectors.mallet'
        output_path = self.db_path + '/topic_model/'
        if not os.path.isdir(output_path):
            os.makedirs(output_path, 0755)
        output_doc_topics = output_path + '/output_doc_topics'
        output_state = output_path + '/output_state.gz'
        command = self.mallet_exec + ' train-topics --input ' + input_file + ' --num-topics '\
                + str(self.topics) + ' --output-doc-topics ' + output_doc_topics + ' --output-state '\
                + output_state + ' --num-threads ' + str(threads) + ' --num-iterations 20000 --random-seed 1'\
                + ' --optimize-interval 10'
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
        positions = {}
        words_in_topic = {}
        for topic in topics:
            if topic not in ordered_topics:
                ordered_topics[topic] = {}
            for pos, word in enumerate(sorted(topics[topic], key=topics[topic].get, reverse=True)):
                ordered_topics[topic][word] = pos
                if word not in positions:
                    positions[word] = []
                positions[word].append((topic, pos))
                if topic not in words_in_topic:
                    words_in_topic[topic] = 0
                words_in_topic[topic] += topics[topic][word]
                
        conn = sqlite3.connect(self.db_path + '/lda_topics.sqlite')
        conn.text_factory = str
        c = conn.cursor()
        
        ## store topics in database
        c.execute('''create table topics (topic int, words text)''')
        c.execute('''create index topic_index on topics(topic)''')
        for topic in topics:
            words_freq = dict([(word, (topics[topic][word] / words_in_topic[topic])) for word in topics[topic]])
            c.execute('insert into topics values (?,?)', (topic, json.dumps(words_freq)))
        conn.commit()
        
        ## Store highest topic for each word in database
        c.execute('''create table word_position (word text, topic int, position int)''')
        c.execute('''create index word_index on word_position(word)''')
        for word in positions:
            for topic, pos in positions[word]:
                c.execute('insert into word_position values (?,?,?)', (word, topic, pos))
        conn.commit()
        c.close()
            
                
    def parse_topics_in_docs(self):
        input_file = self.db_path + '/topic_model/output_doc_topics'
        array_path = self.db_path + '/topic_model/topic_arrays/'
        os.makedirs(array_path)
        path = re.compile('file:' + self.db_path + '/pruned_texts/')
        extension = re.compile('\.txt')
        topic_prop = {}
        for line in open(input_file):
            if re.search('#', line):
                continue
            array = zeros(self.topics, dtype=float32)
            fields = line.split()[1:]
            doc = fields.pop(0)
            doc = path.sub('', doc)
            doc = extension.sub('', doc)
            array_location = array_path + doc + '.npy'
            doc = doc.replace('-', ' ')
            topic_prop[doc] = []
            for pos, field in enumerate(fields):
                if self.isodd(pos):
                    continue
                topic = int(fields[pos])
                proportion = float(fields[pos + 1])
                array[topic] = proportion
            save(array_location, array)
        
    def isodd(self, num):
        """Function taken from http://stackoverflow.com/questions/1089936/even-and-odd-number"""
        return num & 1 and True or False