#!/usr/bin/env python

import re
import sys
import sqlite3
from data_handler import np_array_loader
from glob import glob
from os import makedirs, listdir

          

class Indexer(object):
    """Indexes a philologic database and generates numpy arrays for vector space calculations
    as well as stores word hits in a SQLite table to use for ranked relevance search"""
    
    def __init__(self, db, arrays=True, relevance_ranking=True, store_results=False, stopwords=False, word_cutoff=0, depth=0):
        self.db_path = '/var/lib/philologic/databases/' + db + '/'
        self.docs = glob(self.db_path + 'WORK/*words.sorted')
        self.store_results = store_results
        self.arrays = arrays
        self.r_r = relevance_ranking
        self.depth = depth
        
        self.stopwords = set([])
        if stopwords:
            self.get_stopwords(stopwords)
            
        self.word_ids(word_cutoff)
        
        if self.arrays:
            try:
                from numpy import zeros, float32, save
                self.zeros = zeros
                self.float32 = float32
                self.save = save
                self.word_num = len(self.word_map) + 1 # last column for sum of words in doc
            except ImportError:
                self.arrays = False
                print >> sys.stderr, "numpy is not installed, numpy arrays won't be generated"
            
        if relevance_ranking:
            self.__init__sqlite()
            self.hits_per_word = {}
            
    def get_stopwords(self, path):
        for word in open(path):
            word = word.rstrip()
            self.stopwords.add(word)

    def word_ids(self, word_cutoff):
        """Map words to integers"""
        self.word_map = {}
        endcutoff = len([line for line in open(self.db_path + 'WORK/all.frequencies')]) - word_cutoff
        word_id = 0
        for line_count, line in enumerate(open(self.db_path + 'WORK/all.frequencies')):
            if word_cutoff < line_count < endcutoff:
                word = line.split()[1]
                count = line.split()[0]
                if word not in self.stopwords or count > 10:
                    self.word_map[word] = word_id
                    word_id += 1
        output = open(self.db_path + 'word_num.txt', 'w')
        output.write(str(len(self.word_map)))
        output.close()

    def __init__array(self, sum_of_words):
        """Create numpy arrays"""
        array = self.zeros(self.word_num, dtype=self.float32)
        array[-1] = sum_of_words
        return array
        
    def make_array(self, obj_id, array):
        """Save numpy arrays to disk"""
        name = '-'.join(obj_id.split())
        if self.depth:
            path = self.db_path + 'obj_arrays/'
        else:
            path = self.db_path + 'doc_arrays/'
        array_path = path + name + '.npy'
        try:
            self.save(array_path, array)
        except IOError:
            makedirs(path, 0755)
        
    def __init__sqlite(self):
        """Initialize SQLite connection"""
        self.conn = sqlite3.connect(self.db_path + 'hits_per_word.sqlite')
        self.conn.text_factory = str 
        self.c = self.conn.cursor()
        if self.depth:
            self.c.execute('''create table obj_hits (word text, obj_id text, word_freq int, total_words int)''')
            self.c.execute('''create index word_obj_index on obj_hits(word)''')
        else:
            self.c.execute('''create table doc_hits (word text, doc_id int, word_freq int, total_words int)''')
            self.c.execute('''create index word_doc_index on doc_hits(word)''')
               
    def index_docs(self): ## depth level beyond doc id
        """Index documents using *words.sorted files in the WORK directory of the Philologic database"""
        obj_count = 0
        exclude = re.compile('all.words.sorted')
        for doc in self.docs:
            if exclude.search(doc):
                continue
            print 'one done'
            doc_dict = {}
            endslice = 3 + self.depth
            for line in open(doc):
                fields = line.split()
                word = fields[1]
                if word not in self.word_map:
                    continue
                doc_id = int(fields[2])
                self.doc = doc_id
                
                obj_id = ' '.join(fields[2:endslice])
                
                if obj_id not in doc_dict:
                    doc_dict[obj_id] = {}
                
                if word not in doc_dict[obj_id]:
                    doc_dict[obj_id][word] = 1
                else:
                    doc_dict[obj_id][word] += 1
            
            for obj_id in doc_dict:
                obj_count += 1
                sum_of_words = sum([i for i in doc_dict[obj_id].values()])
                if self.arrays:
                    array = self.__init__array(sum_of_words)
                
                for word in doc_dict[obj_id]:
                    if self.arrays:
                        array[self.word_map[word]] = doc_dict[obj_id][word]
                    if self.r_r:
                        if not self.depth:
                            self.c.execute('insert into doc_hits values (?,?,?,?)', (word, doc_id, doc_dict[obj_id][word], sum_of_words))
                        else:
                            self.c.execute('insert into obj_hits values (?,?,?,?)', (word, obj_id, doc_dict[obj_id][word], sum_of_words))
                
                if self.arrays:
                    self.make_array(obj_id, array)
        
        if self.r_r:
            self.conn.commit()
            self.c.close()
        
        if self.store_results:
            storage = KNN_stored(self.db_path, self.arrays_path)
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
        
        files = listdir(arrays_path)
        pattern = re.compile('(\d+)\.npy')
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
        if self.docs_only:
            self.c.execute('''create table doc_results (doc_id int, neighbor_doc_id int, neighbor_distance real)''')
            self.c.execute('''create index doc_id_index on doc_results(doc_id)''')
            self.c.execute('''create index distance_doc_id_index on doc_results(neighbor_distance)''')
        else:
            self.c.execute('''create table obj_results (obj_id text, neighbor_obj_id int, neighbor_distance real)''')
            self.c.execute('''create index obj_id_index on obj_results(obj_id)''')
            self.c.execute('''create index distance_obj_id_index on obj_results(neighbor_distance)''')
    
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
                k = self.knn(self.db_path, doc=obj, docs_only=self.docs_only, top_words=top_words, lower_words=lower_words)
                k.search(obj)
                for new_obj, distance in k.results:
                    if self.docs_only:
                        self.c.execute('insert into doc_results values (?,?,?)', (obj, new_obj, distance))
                    else:
                        self.c.execute('insert into obj_results values (?,?,?)', (obj, new_obj, distance))
                count +=1
                print '.',
                if count == 100:
                    self.conn.commit()
                    count = 0
        else:
            from scipy.spatial.distance import cosine
            if self.docs_only:
                array_list = [(obj, np_array_loader(obj, self.db_path, top=100, lower=-100)) for obj in self.objects]
            else:
                array_list = [(obj, np_array_loader(obj, self.db_path, docs=False, top=100, lower=-100)) for obj in self.objects]
            for obj, array in array_list:
                for new_obj, new_array in array_list:
                    if obj != new_obj:
                        result = 1 - cosine(array, new_array)
                        if self.docs_only:
                            self.c.execute('insert into doc_results values (?,?,?)', (obj, new_obj, result))
                        else:
                            self.c.execute('insert into obj_results values (?,?,?)', (obj, new_obj, result))
                count += 1
                if count == 100:
                    self.conn.commit()
                    print '.',
                    count = 0
        
        self.conn.commit()
        self.c.close()
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
