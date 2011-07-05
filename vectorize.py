#!/usr/bin/env python

import re
import sys
import sqlite3
from data_handler import np_array_loader
from glob import glob
from os import makedirs

          

class Indexer(object):
    """Indexes a philologic database and generates numpy arrays for vector space calculations
    as well as stores word hits in a SQLite table to use for ranked relevance search"""
    
    def __init__(self, db, arrays=True, ranked_relevance=True, store_results=False, depth=0):
        self.db_path = '/var/lib/philologic/databases/' + db + '/'
        self.docs = glob(self.db_path + 'WORK/*tei.words.sorted')
        self.store_results = store_results
        self.word_ids()
        self.arrays = arrays
        self.r_r = ranked_relevance
        self.depth = depth
        
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
            self.__init__sqlite()
            self.hits_per_word = {}

    def word_ids(self):
        """Map words to integers"""
        self.word_to_id = {}
        word_id = 0
        for line in open(self.db_path + 'WORK/all.frequencies'):
            fields = line.split()
            self.word_to_id[fields[1]] = word_id
            word_id += 1

    def __init__array(self, sum_of_words):
        """Create numpy arrays"""
        self.doc_array = self.zeros(self.word_num, dtype=self.float32)
        self.doc_array[-1] = sum_of_words
        
    def make_array(self, obj_id):
        """Save numpy arrays to disk"""
        name = '-'.join(obj_id.split())
        if self.depth:
            path = self.db_path + 'obj_arrays/'
        else:
            path = self.db_path + 'doc_arrays/'
        array_path = path + name + '.npy'
        try:
            self.save(array_path, self.doc_array)
        except:
            makedirs(path, 0755)
        
    def __init__sqlite(self):
        """Initialize SQLite connection"""
        self.conn = sqlite3.connect(self.db_path + 'hits_per_word.sqlite')
        self.c = self.conn.cursor()
        if self.depth:
            self.c.execute('''create table obj_hits (word int, obj_id text, word_freq int, total_words int)''')
            self.c.execute('''create index word_obj_index on obj_hits(word)''')
        else:
            self.c.execute('''create table doc_hits (word int, doc_id int, word_freq int, total_words int)''')
            self.c.execute('''create index word_doc_index on doc_hits(word)''')
               
    def index_docs(self): ## depth level beyond doc id
        """Index documents using *words.sorted files in the WORK directory of the Philologic database"""
        obj_count = 0
        exclude = re.compile('all.words.sorted')
        for doc in self.docs:
            if exclude.search(doc):
                continue
            obj_count += 1
            doc_dict = {}
            endslice = 3 + self.depth
            for line in open(doc):
                fields = line.split()
                word_id = int(self.word_to_id[fields[1]])
                doc_id = int(fields[2])
                
                obj_id = ' '.join(fields[2:endslice])
                
                if obj_id not in doc_dict:
                    doc_dict[obj_id] = {}
                
                if word_id not in doc_dict[obj_id]:
                    doc_dict[obj_id][word_id] = 1
                else:
                    doc_dict[obj_id][word_id] += 1

            for obj_id in doc_dict:
                obj_count += 1
                sum_of_words = sum([i for i in doc_dict[obj_id].values()])
                if self.arrays:
                    self.__init__array(sum_of_words)
                
                for word_id in doc_dict[obj_id]:
                    if self.arrays:
                        self.doc_array[word_id] = doc_dict[obj_id][word_id]
                    if self.r_r:
                        if not self.depth:
                            self.c.execute('insert into doc_hits values (?,?,?,?)', (word_id, doc_id, doc_dict[obj_id][word_id], sum_of_words))
                        else:
                            self.c.execute('insert into obj_hits values (?,?,?,?)', (word_id, obj_id, doc_dict[obj_id][word_id], sum_of_words))
                
                if self.arrays:
                    self.make_array(obj_id)
                
            if self.r_r:
                if obj_count > 100:
                    self.conn.commit()
                    print '.',
                    obj_count = 0
        
        if self.r_r:
            self.conn.commit()
            self.c.close()
        
        if self.store_results:
            storage = VSM_stored(self.db_path, self.arrays_path)
            storage.store_results()


class KNN_stored(object):
    """Class used to store distances between numpy arrays"""
    
    
    def __init__(self, db_path, arrays_path, docs_only=True, high_ram=False):
        """The docs_only option lets you specifiy which type of objects you want to generate results for, 
        full documents, or individual divs.
        The high_ram option lets you specify which method to use for getting those results, on disk or in memory"""
        try:
            from knn_helper import knn
            self.knn = knn
        except ImportError:
            print >> sys.stderr, "scipy is not installed, KNN results will not be stored"
        
        files = glob(arrays_path + '*')
        pattern = re.compile(arrays_path + '(\d+)\.npy')
        divs = re.compile('-')
        if docs_only:
            self.objects = [int(pattern.sub('\\1', doc)) for doc in files if not divs.search(doc)]
        else:
            self.objects = [pattern.sub('\\1', doc) for doc in files if divs.search(doc)]
        self.db_path = db_path
        self.arrays_path = arrays_path
        self.in_mem = high_ram
        self.docs_only = docs_only
        
    def __init__sqlite(self):
        self.conn = sqlite3.connect(self.db_path + 'knn_results.sqlite')
        self.c = self.conn.cursor()
        if self.objects_only:
            self.c.execute('''create table doc_results (doc_id int, neighbor_doc_id int, neighbor_distance real)''')
            self.c.execute('''create index doc_id_index on results(doc_id)''')
            self.c.execute('''create index distance_doc_id_index on results(neighbor_distance)''')
        else:
            self.c.execute('''create table obj_results (obj_id text, neighbor_obj_id int, neighbor_distance real)''')
            self.c.execute('''create index obj_id_index on results(obj_id)''')
            self.c.execute('''create index distance_obj_id_index on results(neighbor_distance)''')
    
    def store_results(self):
        """Two methods to generate results:
        - The first one reads all numpy arrays of disk for each document:
        This has the advantage of not using a lot of memory, but at the cost of performance
        - The second one reads all numpy arrays once, thereby being much faster, but at the cost
        of memory usage which skyrockets."""
        self.__init__sqlite()
        top_words = 100
        lower_words = -100
        count = 0
        if not self.in_mem:
            for obj in self.objects:
                k = self.knn(self.db_path, doc=obj, top_words=top_words, lower_words=lower_words)
                k.search()
                for new_doc, distance in k.results:
                    if self.docs_only:
                        self.c.execute('insert into doc_results values (?,?,?)', (obj, new_obj, distance))
                    else:
                        self.c.execute('insert into obj_results values (?,?,?)', (obj, new_obj, distance))
                count +=1
                if count == 10:
                    print '.',
                    count = 0
                self.conn.commit()
        else:
            from scipy.spatial.distance import cosine
            array_list = [(obj, np_array_loader(obj, self.db_path, top=top_words, lower=lower_words)) for obj in self.objects]
            for obj, array in array_list:
                for new_obj, new_array in array_list:
                    if obj != new_obj:
                        result = 1 - cosine(array, new_array)
                        if self.docs_only:
                            self.c.execute('insert into doc_results values (?,?,?)', (obj, new_obj, result))
                        else:
                            self.c.execute('insert into obj_results values (?,?,?)', (obj, new_obj, result))
                self.conn.commit()
                count += 1
                if count == 10:
                    print '.',
                    count = 0
        
        self.c.close()
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        