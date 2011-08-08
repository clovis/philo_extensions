#!/usr/bin/env python

from operator import itemgetter
from scipy.spatial.distance import *
from data_handler import *
from word_mapper import mapper



class knn(object):
    
    def __init__(self, db_path, doc=None, docs=None, docs_only=True, stored=False, top_words=100, lower_words=-100, metric=cosine):
        self.id_to_word = mapper(db_path + '/WORK/all.frequencies')
        self.stored = stored
        self.docs_only = docs_only
        if stored:
            self.db = db_path + 'vsm_results.sqlite'
            self.conn = sqlite3.connect(self.db)
            self.cursor = self.conn.cursor()
        else:
            self.top = top_words
            self.lower = lower_words
            self.orig = self.loader(doc, db_path, top_words, lower_words)
            self.metric = metric
            self.path = db_path
            self.results = []
            ## Leave the option to specify a certain subset of docs
            if docs == None:
                self.docs = doc_enumerator(db_path, docs_only=docs_only)
            else:
                self.docs = docs
            
    def search(self, doc_id, display=10):
        if self.stored:
            query = 'select doc_id, neighbor_doc_id, neighbor_distance from doc_results where doc_id = %d or neighbor_doc_id = %d order by neighbor_distance desc limit %d' % (doc_id, doc_id, display)
            self.cursor.execute(query)
            self.results = []
            for doc, other_doc, distance in self.cursor.fetchall():
                if doc != doc_id:
                    self.results.append((doc, distance))
                else:
                    self.results.append((other_doc, distance))
            return results
        else:
            for doc_id in self.docs:
                doc = np_array_loader(doc_id, self.path, docs_only=self.docs_only, top=100, lower=-100)
                self.results.append((doc_id, self.distance(doc)))
            return sorted(self.results, key=itemgetter(1), reverse=True)[:display]
    
    def distance(self, doc):
        return 1 - self.metric(self.orig, doc)
        
    def loader(self, doc, path, top_words, lower_words):
        return np_array_loader(doc, path, docs_only=self.docs_only, top=top_words, lower=lower_words)
        
    def sort_results(self, results, order=True, display=-1):
        return sorted(self.results, key=itemgetter(1), reverse=order)[:display]
        
    def word_freq_sort(self, doc, path):
        # exclude last column in row since it doesn't exist in word mappings
        mydoc = np_array_loader(doc, path)
        frequencies = [(self.id_to_word[word], mydoc[word]) for word in mydoc[:-1].nonzero()[0]]
        return sorted(frequencies, key=itemgetter(1), reverse=True)[:10]