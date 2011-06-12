#!/usr/bin/env python

from glob import glob
from os import makedirs
import sys
import sqlite3


class Indexer(object):
    
    
    def __init__(self, db, arrays=True, ranked_relevance=True):
        self.db_path = '/var/lib/philologic/databases/' + db + '/'
        self.docs = glob(self.db_path + 'WORK/*words.sorted')
        self.arrays_path = self.db_path + 'doc_arrays/'
        self.word_ids()
        
        if arrays:
            try:
                from numpy import zeros, float32, save
                self.zeros = zeros
                self.float32 = float32
                self.save = save
                self.arrays = arrays
                self.word_num = len(self.word_to_id) + 1 # last column for sum of words in doc
            except ImportError:
                self.arrays = False
                print >> sys.stderr, "numpy is not installed, numpy arrays won't be generated"
            
        if ranked_relevance:
            self.r_r = ranked_relevance
            self.__init__sqlite()
            self.hits_per_word = {}
        
        makedirs(self.arrays_path, 0755)

    def word_ids(self):
        self.id_to_word = {}
        self.word_to_id = {}
        word_id = 0
        for line in open(self.db_path + 'WORK/all.frequencies'):
            fields = line.split()
            self.word_to_id[fields[1]] = word_id
            self.id_to_word[word_id] = fields[1]
            word_id += 1

    def __init__array(self, sum_of_words):
        self.doc_array = self.zeros(self.word_num, dtype=self.float32)
        self.doc_array[-1] = sum_of_words
        
    def make_array(self, doc_id):
        array_path = self.arrays_path + doc_id + '.npy'
        self.save(array_path, self.doc_array)
        
    def __init__sqlite(self):
        self.conn = sqlite3.connect(self.db_path + 'hits_per_word.sqlite')
        self.c = self.conn.cursor()
        self.c.execute('''create table hits (word int, docs blob)''')
        self.c.execute('''create index word_index on hits(word)''')
        
    def create_row(self, word_id):
        self.c.execute('insert into hits values (?,?)', (word_id, ''))
    
    def store_frequencies(self, word_id, value):
        self.c.execute('select docs from hits where word=?', (word_id,))
        old_value = self.c.fetchone()[0]
        new_value = old_value + '/' + value
        self.c.execute('update hits set docs=? where word=?', (new_value, word_id))
               
    def index_docs(self):
        count = 0
        for doc in self.docs:
            count += 1
            doc_dict = {}
            doc_id = ''
            for line in open(doc):
                fields = line.split()
                word_id = self.word_to_id[fields[1]]
                doc_id = fields[2]
                if word_id not in doc_dict:
                    doc_dict[word_id] = 1
                else:
                    doc_dict[word_id] += 1
            
            sum_of_words = sum([i for i in doc_dict.values()])
            
            if self.arrays:
                self.__init__array(sum_of_words)
                
            for word_id in doc_dict:
                if self.arrays:
                    self.doc_array[int(word_id)] = doc_dict[word_id]
                if self.r_r:
                    if word_id not in self.hits_per_word:
                        self.hits_per_word[word_id] = 1
                        self.create_row(word_id)
                    value = doc_id + ',' + str(doc_dict[word_id]) + ',' + str(sum_of_words)
                    self.store_frequencies(word_id, value)
            
            del doc_dict
            
            if self.arrays:
                self.make_array(doc_id)
                
            if self.r_r:
                if count == 100:
                    self.conn.commit()
                    count = 0
        
        if self.r_r:
            self.conn.commit()
            self.c.close()