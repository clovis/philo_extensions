#!/usr/bin/env python

import re
import sys
import sqlite3
from data_handler import np_array_loader
from glob import glob
from os import makedirs, listdir
from operator import itemgetter

          

class Indexer(object):
    """Indexes a philologic database and generates numpy arrays for vector space calculations
    as well as stores word hits in a SQLite table to use for ranked relevance search"""
    
    def __init__(self, db, arrays=True, relevance_ranking=True, store_results=False, stopwords=False, stemmer=False, 
                word_cutoff=0, min_freq=10, min_words=100, max_words=None, depth=0):
        self.db_path = '/var/lib/philologic/databases/' + db + '/'
        self.docs = glob(self.db_path + 'WORK/*words.sorted')
        self.store_results = store_results
        self.arrays = arrays
        self.r_r = relevance_ranking
        self.depth = depth
        self.min_words = min_words
        if max_words == None:
            self.max_words = sum([int(line.split()[0]) for line in open(self.db_path + 'WORK/all.frequencies')])
        else:
            self.max_words = max_words
        self.stopwords = self.get_stopwords(stopwords)
        self.stemmer = self.load_stemmer(stemmer)       
        self.word_ids(word_cutoff, min_freq)
        
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
            
    def load_stemmer(self, stemmer):
        if stemmer:
            try:
                from Stemmer import Stemmer
                return Stemmer(stemmer) # where stemmer is the language selected
            except KeyError:
                print >> sys.stderr, "Language not supported by stemmer. No stemming will be done."
            except ImportError:
                print >> sys.stderr, "PyStemmer is not installed on your system. No stemming will be done."
        else:
            return False
            
    def get_stopwords(self, stopwords):
        stopword_list = set([])
        if stopwords:
            for word in open(stopwords):
                word = word.rstrip()
                self.stopword_list.add(word)
        return stopword_list

    def word_ids(self, word_cutoff, min_freq):
        """Map words to integers"""
        self.word_map = {}
        endcutoff = len([line for line in open(self.db_path + 'WORK/all.frequencies')]) - word_cutoff
        word_id = 0
        for line_count, line in enumerate(open(self.db_path + 'WORK/all.frequencies')):
            if word_cutoff < line_count < endcutoff:
                word = line.split()[1]
                count = int(line.split()[0])
                if word not in self.stopwords and count > min_freq:
                    if self.stemmer:
                        word = self.stemmer.stemWord(word)
                    if word not in self.word_map:
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
                if self.stemmer:
                    word = self.stemmer.stemWord(word)
                if word in self.word_map:
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
                sum_of_words = sum([i for i in doc_dict[obj_id].values()])
                if self.arrays and self.min_words < sum_of_words < self.max_words:
                    array = self.__init__array(sum_of_words)
                
                for word in doc_dict[obj_id]:
                    if self.arrays and self.min_words < sum_of_words < self.max_words:
                        array[self.word_map[word]] = doc_dict[obj_id][word]
                    if self.r_r:
                        if not self.depth:
                            self.c.execute('insert into doc_hits values (?,?,?,?)', (word, doc_id, doc_dict[obj_id][word], sum_of_words))
                        else:
                            self.c.execute('insert into obj_hits values (?,?,?,?)', (word, obj_id, doc_dict[obj_id][word], sum_of_words))
                
                if self.arrays and self.min_words < sum_of_words < self.max_words:
                    self.make_array(obj_id, array)
        
        if self.r_r:
            self.conn.commit()
            self.c.close()
        
        if self.store_results:
            storage = KNN_stored(self.db_path, self.arrays_path)
            storage.store_results()


class KNN_stored(object):
    """Class used to store distances between numpy arrays"""
    
    
    def __init__(self, db_path, arrays_path, docs_only=True, limit_results=100):
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
        self.docs_only = docs_only
        self.limit = limit_results
        
    def __init__sqlite(self):
        self.conn = sqlite3.connect(self.db_path + 'knn_results.sqlite')
        self.c = self.conn.cursor()
        if self.docs_only:
            self.c.execute('''create table doc_results (doc_id int, neighbor_doc_id int, neighbor_distance real)''')
            self.c.execute('''create index doc_id_index on doc_results(doc_id)''')
            self.c.execute('''create index distance_doc_id_index on doc_results(neighbor_distance)''')
        else:
            self.c.execute('''create table obj_results (obj_id text, neighbor_obj_id text, neighbor_distance real)''')
            self.c.execute('''create index obj_id_index on obj_results(obj_id)''')
            self.c.execute('''create index distance_obj_id_index on obj_results(neighbor_distance)''')
    
    def write_to_disk(self, results):
        for obj, new_obj, result in results:
            if self.docs_only:
                self.c.execute('insert into doc_results values (?,?,?)', (obj, new_obj, result))
            else:
                self.c.execute('insert into obj_results values (?,?,?)', (obj, new_obj, result))
        self.conn.commit()
    
    def store_results(self):
        """This will load all numpy arrays saved on disk and compute the cosine distance for each
        array in the corpus"""
        from scipy.spatial.distance import cosine
        self.__init__sqlite()
        results = []
        count = 0
        one = 0
        if self.docs_only:
            array_list = [(obj, np_array_loader(obj, self.db_path)) for obj in self.objects]
        else:
            array_list = [(obj, np_array_loader(obj, self.db_path, docs_only=False)) for obj in self.objects]
        ten_percent = len(array_list)/10
        one_percent = len(array_list)/100
        for obj, array in array_list:
            full_results = []
            for new_obj, new_array in array_list:
                if obj != new_obj:
                    result = 1 - cosine(array, new_array)
                    full_results.append((obj, new_obj, result))
            results += sorted(full_results, key=itemgetter(2), reverse=True)[:self.limit]
            count += 1
            one += 1
            if count > ten_percent:
                print '+',
                self.write_to_disk(results)
                count = 0
                one = 0
                results = []
            elif one > one_percent:
                print '.',
                one = 0
        print 'done with calculations...writing last bits to disk...'
        for obj, new_obj, result in results:
            if self.docs_only:
                self.c.execute('insert into doc_results values (?,?,?)', (obj, new_obj, result))
            else:
                self.c.execute('insert into obj_results values (?,?,?)', (obj, new_obj, result))
    
        self.conn.commit()
        self.c.close()
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
