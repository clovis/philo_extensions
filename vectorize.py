#!/usr/bin/env python

from __future__ import division
import re
import sys
import sqlite3
from data_handler import np_load
from glob import glob
from os import makedirs, listdir, path, fork, waitpid
from shutil import rmtree
from operator import itemgetter
from cPickle import load, dump

          

class Indexer(object):
    """Indexes a philologic database and generates numpy arrays for vector space calculations
    as well as stores word hits in a SQLite table to use for ranked relevance search"""
    
    def __init__(self, db, arrays=True, relevance_ranking=True, save_text=False, store_results=False, stopwords=False, stemmer=False, 
                word_cutoff=0, min_freq=10, min_words=0, max_words=None, min_percent=0, max_percent=100, depth=0):
        """The depth variable defines how far to go in the tree. The value 0 corresponds to the doc level"""
        
        self.db_path = '/var/lib/philologic/databases/' + db + '/'
        self.docs = glob(self.db_path + 'WORK/*words.sorted')
        self.store_results = store_results
        self.arrays = arrays
        self.r_r = relevance_ranking
        self.save_docs = save_text
        if save_text:
            self.save_docs = save_text
            self.text_path = self.db_path + 'pruned_texts/'
            if not path.isdir(self.text_path):
                makedirs(self.text_path, 0755)
        self.depth = depth
        self.min_words = min_words
        if max_words == None:
            self.max_words = sum([int(line.split()[0]) for line in open(self.db_path + 'WORK/all.frequencies')])
        else:
            self.max_words = max_words
        self.stemmer = self.load_stemmer(stemmer)
        self.word_occurence_in_corpus(min_percent, max_percent)
        self.stopwords = self.get_stopwords(stopwords)
        self.word_ids(word_cutoff, min_freq)
        
        if self.arrays:
            try:
                from numpy import zeros, float32, save
                self.zeros = zeros
                self.float32 = float32
                self.save = save
                self.word_num = len(self.word_map)
                self.array_path = self.db_path + 'obj_arrays/'
                if not path.isdir(self.array_path):
                    makedirs(self.array_path, 0755)
                elif listdir(self.array_path) != []:
                    print 'There are files from a previous run in the %s directory.' % self.array_path
                    print 'Please delete them and rerun this script.'
                    sys.exit()
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
                if self.stemmer:
                    word = self.stemm(word)
                stopword_list.add(word)
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
                if self.stemmer:
                    word = self.stemm(word)
                if word not in self.stopwords and count > min_freq and word in self.words_to_keep:
                    if word not in self.word_map and len(word) > 1:
                        self.word_map[word] = word_id
                        word_id += 1
        output = open(self.db_path + 'word_num.txt', 'w')
        output.write(str(len(self.word_map)))
        output.close()
        
    def word_occurence_in_corpus(self, min_percent, max_percent):
        word_occurence = {}
        exclude = re.compile('all.words.sorted')
        endslice = 3 + self.depth
        obj_num = set([])
        for doc in self.docs:
            if exclude.search(doc):
                continue
            for line in open(doc):
                fields = line.split()
                word = fields[1]
                if self.stemmer:
                    word = self.stemm(word)
                obj_id = ' '.join(fields[2:endslice])
                if word not in word_occurence:
                    word_occurence[word] = set([])
                word_occurence[word].add(obj_id)
                obj_num.add(obj_id)
        doc_num = len(obj_num)
        self.words_to_keep = set([])
        for word in word_occurence:
            if min_percent < (len(word_occurence[word]) / doc_num * 100) < max_percent:
                self.words_to_keep.add(word)
        print len(self.words_to_keep)
        
    def stemm(self, word):
        try:
            word = self.stemmer.stemWord(word)
        except UnicodeDecodeError:
            word = self.stemmer.stemWord(word.decode('latin-1').encode('utf-8'))
        return word

    def __init__array(self):
        """Create numpy arrays"""
        array = self.zeros(self.word_num, dtype=self.float32)
        return array
        
    def make_array(self, obj_id, array):
        """Save numpy arrays to disk"""
        name = '-'.join(obj_id.split())
        array_location = self.array_path + name + '.npy'
        self.save(array_location, array)
        
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
            
    def save_text(self, doc_dict):
        for obj in doc_dict:
            text = ''
            for word in doc_dict[obj]:
                words = ' '.join([word for i in range(doc_dict[obj][word])])
                text +=  words + ' '
            obj = '-'.join(obj.split())
            output = open(self.text_path + obj + '.txt', 'w')
            output.write(text)
            
    def index_docs(self): 
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
                    word = self.stemm(word)
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
            
            
            if self.save_docs:
                self.save_text(doc_dict)
            for obj_id in doc_dict:
                word_count = sum([i for i in doc_dict[obj_id].values()])
                
                ## Check if arrays are to be generated
                if self.arrays and self.min_words < word_count < self.max_words:
                    array = self.__init__array()
                
                ## Iterate through each word in the doc and populate arrays
                ## and insert values in SQLite table
                for word in doc_dict[obj_id]:
                    if self.arrays and self.min_words < word_count < self.max_words:
                        array[self.word_map[word]] = doc_dict[obj_id][word]
                    if self.r_r:
                        if not self.depth:
                            self.c.execute('insert into doc_hits values (?,?,?,?)', (word, doc_id, doc_dict[obj_id][word], word_count))
                        else:
                            self.c.execute('insert into obj_hits values (?,?,?,?)', (word, obj_id, doc_dict[obj_id][word], word_count))
                
                ## Save array only if the word count is higher than self.min_words
                ## and less then self.max_words
                if self.arrays and self.min_words < word_count < self.max_words:
                    self.make_array(obj_id, array)
        
        if self.r_r:
            self.conn.commit()
            self.c.close()
        
        if self.store_results:
            storage = KNN_stored(self.db_path, self.arrays_path)
            storage.store_results()


class KNN_stored(object):
    """Class used to store distances between numpy arrays"""
    
    
    def __init__(self, db, dir_path='/var/lib/philologic/databases/', measure='cosine', dbfile_name=False, limit_results=100, workers=2,
                use_lda=False, use_only_lda=False):
        """The docs_only option lets you specifiy which type of objects you want to generate results for, 
        full documents, or individual divs."""
        try:
            import scipy.spatial.distance
            #self.distance = scipy.spatial.distance
            self.measure = getattr(scipy.spatial.distance, measure)
        except ImportError:
            print >> sys.stderr, "scipy is not installed, KNN results will not be stored"
        
        self.db_path = dir_path + db + '/'
        self.limit = limit_results
        self.workers = workers
        self.lda = use_lda
        
        if dbfile_name:
            self.db_file = self.db_path + dbfile_name
        elif use_only_lda:
            self.db_file = self.db_path + measure + '_lda_only_distance_results.sqlite'
        elif use_lda:
            self.db_file = self.db_path + measure + '_with_lda_distance_results.sqlite'
        else:
            self.db_file = self.db_path + measure + '_distance_results.sqlite'
        count = 0
        while path.isfile(self.db_file):
            print '%s already exists' % self.db_file
            count += 1
            self.db_file = re.sub('\d*\.sqlite', str(count) + '.sqlite', self.db_file)
            print 'renaming to %s' % self.db_file
        
        if use_only_lda:
            arrays_path = self.db_path + '/topic_model/topic_arrays/'
        else:
            arrays_path = self.db_path + 'obj_arrays/'
        files = listdir(arrays_path)
        objects = [doc.replace('.npy', '') for doc in files]
        self.array_list = [(obj.replace('-', ' '), np_load(obj, arrays_path)) for obj in objects]
        
        if use_lda and not use_only_lda:
            array_path = self.db_path + '/topic_model/topic_arrays/'
            files = listdir(array_path)
            objects = [doc.replace('.npy', '') for doc in files]
            self.topic_distribution = dict([(obj.replace('-', ' '), np_load(obj, array_path, normalize=False)) for obj in objects])
        
    def __init__sqlite(self):        
        self.conn = sqlite3.connect(self.db_file)
        self.c = self.conn.cursor()
        self.c.execute('''create table obj_results (obj_id text, neighbor_obj_id text, neighbor_distance real)''')
        self.c.execute('''create index obj_id_index on obj_results(obj_id)''')
        self.c.execute('''create index distance_obj_id_index on obj_results(neighbor_distance)''')
    
    def write_to_disk(self, obj, results, temp_dir):
        obj = obj.replace(' ', '-') + '.pickle'
        output = open(temp_dir + obj, 'w')
        dump(results, output, -1)
    
    def store_results(self):
        """This will load all numpy arrays saved on disk and compute the cosine distance for each
        array in the corpus"""
        
        self.__init__sqlite()
        temp_dir = self.db_path + 'temp_results/'
        makedirs(temp_dir, 0755)
        results = []
        total = len(self.array_list)
        done = 0
        workers = 0
        arrays = range(total)
        while done < total:
            while arrays and workers < self.workers:
                obj, array = self.array_list[arrays.pop(0)]
                pid = fork()
                if pid:
                    workers += 1
                if not pid:
                    full_results = []
                    for new_obj, new_array in self.array_list:
                        if obj != new_obj:
                            result = 1 - self.measure(array, new_array)
                            if self.lda:
                                result *= 1 - self.measure(self.topic_distribution[obj], self.topic_distribution[new_obj])
                            full_results.append((obj, new_obj, result))
                    results = sorted(full_results, key=itemgetter(2), reverse=True)[:self.limit]
                    self.write_to_disk(obj, results, temp_dir)
                    exit()
            pid,status = waitpid(0,0)
            workers -= 1
            done += 1
           
        for file in glob(temp_dir + '*'):
            results = load(open(file))
            for obj, new_obj, result in results:
                self.c.execute('insert into obj_results values (?,?,?)', (obj, new_obj, result))
        self.conn.commit()
        self.c.close()
        rmtree(temp_dir)
        
        
        

        
        
        
