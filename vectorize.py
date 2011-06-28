#!/usr/bin/env python

from glob import glob
from os import makedirs
import sys
import sqlite3
from data_handler import np_array_loader


class Indexer(object):
    
    
    def __init__(self, db, arrays=True, ranked_relevance=True, store_results=False):
        self.db_path = '/var/lib/philologic/databases/' + db + '/'
        self.docs = glob(self.db_path + 'WORK/*words.sorted')
        self.arrays_path = self.db_path + 'doc_arrays/'
        self.store_results = store_results
        self.word_ids()
        self.arrays = arrays
        
        if self.arrays:
            try:
                from numpy import zeros, float32, save
                self.zeros = zeros
                self.float32 = float32
                self.save = save
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
        self.c.execute('''drop table if exists hits''')
        self.c.execute('''create table hits (word int, doc_id int, word_freq int, total_words int)''')
        self.c.execute('''create index word_index on hits(word)''')
               
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
                    self.c.execute('insert into hits values (?,?,?,?)', (word_id, int(doc_id), doc_dict[word_id], sum_of_words))
            
            del doc_dict
            
            if self.arrays:
                self.make_array(doc_id)
                
            if self.r_r:
                if count == 100:
                    self.conn.commit()
                    print '.',
                    count = 0
        
        if self.r_r:
            self.conn.commit()
            self.c.close()
        
        if self.store_results:
            storage = VSM_stored(self.db_path, self.arrays_path)
            storage.store_results()
            
            
class VSM_stored(object):
    
    
    def __init__(self, db_path, arrays_path, high_ram=False):
        try:
            from knn_helper import knn
            self.knn = knn
        except ImportError:
            print >> sys.stderr, "scipy is not installed, VSM results will not be stored"
        
        import re
        pattern = re.compile(arrays_path + '(\d+)\.npy')
        docs = glob(arrays_path + '*')
        self.docs = [int(pattern.sub('\\1', doc)) for doc in docs]
        self.db_path = db_path
        self.arrays_path = arrays_path
        self.in_mem = high_ram
        
    def __init__sqlite(self):
        self.conn = sqlite3.connect(self.db_path + 'vsm_results.sqlite')
        self.c = self.conn.cursor()
        self.c.execute('''drop table if exists results''')
        self.c.execute('''create table results (doc_id int, neighbor_doc_id int, neighbor_distance real)''')
        self.c.execute('''create index doc_id_index on results(doc_id)''')
        self.c.execute('''create index distance_id_index on results(neighbor_distance)''')
    
    def store_results(self):
        self.__init__sqlite()
        top_words = 100
        lower_words = -100
        count = 0
        if not self.in_mem:
            for doc in self.docs:
                k = self.knn(doc, self.db_path, top_words=top_words, lower_words=lower_words)
                k.search()
                for new_doc, distance in k.results:
                    self.c.execute('insert into results values (?,?,?)', (doc, new_doc, distance))
                count +=1
                if count == 10:
                    print '.',
                    count = 0
                self.conn.commit()
        else:
            from scipy.spatial.distance import cosine
            array_list = [(doc, np_array_loader(doc, self.db_path, top=top_words, lower=lower_words)) for doc in self.docs]
            for doc, array in array_list:
                for new_doc, new_array in array_list:
                    if doc != new_doc:
                        result = 1 - cosine(array, new_array)
                        self.c.execute('insert into results values (?,?,?)', (doc, new_doc, result))
                self.conn.commit()
                count += 1
                if count == 10:
                    print '.',
                    count = 0
        
        self.c.close()
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        