#!/usr/bin/env python

from math import log, floor
from operator import itemgetter
from word_mapper import mapper
from data_handler import *


class Searcher(object):
    """Run a search on documents or objects within documents
    in the SQLite table
    Three scoring options are available: Frequency, TF-IDF and BM25
    Two methods of incrementing the scores of results are available:
    simple addition or best score"""
    
    
    def __init__(self, query, db, doc_level_search=True, path='/var/lib/philologic/databases/'):
        self.path = path + db + '/'
        self.words = self.word_to_id(query)
        self.doc_level_search = doc_level_search
        self.results = {}        
        
    def get_hits(self, word, doc=True):
        """Query the SQLite table and return a list of tuples containing the results"""
        cursor = sqlite_conn(self.path)
        if self.doc_level_search:
            cursor.execute('select doc_id, word_freq, total_words from doc_hits where word=?', (word,))
        else:
            cursor.execute('select obj_id, word_freq, total_words from obj_hits where word=?', (word,))
        return cursor.fetchall()
        
    def word_to_id(self, query):
        """Return the ID of the word in order to query the SQLite table"""
        m = mapper(self.path)
        words = []
        for word in query.split():
            word_info = m.id_and_freq(word)
            if word_info != None:
                words.append(word_info)
        return words
        
    def id_to_word(self, id):
        """Return the word given its ID"""
        m = mapper(self.path)
        return m[id]
        
    def get_idf(self, hits):
        """Return IDF score"""
        total_docs = doc_counter(self.path)
        return log(float(total_docs) / float(len(hits))) + 1
               
    def search(self, measure='tf_idf', scoring='simple_scoring', intersect=False, display=10):
        """Searcher function"""
        self.intersect = False
        if self.words != []:
            for word in self.words:
                term, term_freq = word
                hits = self.get_hits(term)
                getattr(self, measure)(term_freq, hits, scoring)
                if intersect:
                    if self.intersect:
                        self.docs = self.docs.intersection(self.new_docs)
                        self.new_docs = set([])
                    else:
                        self.intersect = True
                        self.docs = set([obj_id for obj_id in self.results])
                        self.new_docs = set([])
            if intersect:
                self.results = dict([(obj_id, self.results[obj_id]) for obj_id in self.results if obj_id in self.docs])
            return sorted(self.results.iteritems(), key=itemgetter(1), reverse=True)[:display]
        else:
            return []
    
    def tf_idf(self, term_freq, hits, scoring):
        idf = self.get_idf(hits)
        for obj_id, word_freq, word_sum in hits:
            tf = float(word_freq) / float(word_sum)
            score = tf * idf
            getattr(self, scoring)(obj_id, score)
                    
    def frequency(self, term_freq, hits, scoring):
        for obj_id, word_freq, word_sum in hits:
            score = float(word_freq) / float(word_sum)
            getattr(self, scoring)(obj_id, score)
                    
    def bm25(self, term_freq, hits, scoring, k1=1.2, b=0.75):
        ## a floor is applied to normalized length of doc
        ## in order to diminish the importance of small docs
        ## see http://xapian.org/docs/bm25.html
        idf = self.get_idf(hits)
        avg_dl = avg_doc_length(self.path)
        for obj_id, word_freq, obj_length in hits:
            tf = float(word_freq)
            dl = float(obj_length)
            temp_score = tf * (k1 + 1.0)
            temp_score2 = tf + k1 * ((1.0 - b) + b * floor(dl / avg_dl))
            score = idf * temp_score / temp_score2
            getattr(self, scoring)(obj_id, score)
                    
    def simple_scoring(self, obj_id, score):
        if self.intersect:
            self.new_docs.add(obj_id)
        if obj_id not in self.results:
            self.results[obj_id] = score
        else:
            self.results[obj_id] += score
    
    def dismax_scoring(self, obj_id, score):
        if self.intersect:
            self.new_docs.add(obj_id)
        if obj_id not in self.results:
            self.results[obj_id] = score
        else:
            if score > self.results[obj_id]:
                self.results[obj_id] = score