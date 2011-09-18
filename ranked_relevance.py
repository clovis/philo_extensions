#!/usr/bin/env python

import json
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
    
    
    def __init__(self, query, db, doc_level_search=True, stemmer=False, path='/var/lib/philologic/databases/'):
        self.path = path + db + '/'
        self.words = query.split()
        self.doc_level_search = doc_level_search
        self.results = {}
        if doc_level_search:
             self.doc_path = self.path + 'doc_arrays/'
        else:
            self.doc_path = self.path + 'obj_arrays/'
        self.stemmer = stemmer
        if stemmer:
            try:
                from Stemmer import Stemmer
                self.stemmer = Stemmer(stemmer) # where stemmer is the language selected
                self.words = [self.stemmer.stemWord(word) for word in self.words]
            except KeyError:
                print >> sys.stderr, "Language not supported by stemmer. No stemming will be done."
            except ImportError:
                print >> sys.stderr, "PyStemmer is not installed on your system. No stemming will be done."            
        
    def get_hits(self, word, doc=True):
        """Query the SQLite table and return a list of tuples containing the results"""
        cursor = sqlite_conn(self.path + 'hits_per_word.sqlite')
        if self.doc_level_search:
            cursor.execute('select doc_id, word_freq, total_words from doc_hits where word=?', (word,))
        else:
            cursor.execute('select obj_id, word_freq, total_words from obj_hits where word=?', (word,))
        return cursor.fetchall()
        
    def id_to_word(self, id):
        """Return the word given its ID"""
        m = mapper(self.path)
        return m[id]
        
    def get_idf(self, hits):
        """Return IDF score"""
        total_docs = doc_counter(self.doc_path) #### WRONG COUNT
        try:
            return log(float(total_docs) / float(len(hits))) + 1
        except ZeroDivisionError:
            return 0
               
    def search(self, measure='tf_idf', scoring='simple_scoring', intersect=False, display=10):
        """Searcher function"""
        self.intersect = False
        if self.words != []:
            for word in self.words:
                hits = self.get_hits(word)
                getattr(self, measure)(hits, scoring)
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
    
    def debug_score(self, hits, scoring):
        for obj_id, word_freq, word_sum in hits:
            getattr(self, scoring)(obj_id, word_freq)
    
    def tf_idf(self, hits, scoring):
        idf = self.get_idf(hits)
        for obj_id, word_freq, word_sum in hits:
            tf = float(word_freq) / float(word_sum)
            score = tf * idf
            getattr(self, scoring)(obj_id, score)
                    
    def frequency(self, hits, scoring):
        for obj_id, word_freq, word_sum in hits:
            score = float(word_freq) / float(word_sum)
            getattr(self, scoring)(obj_id, score)
                    
    def bm25(self, hits, scoring, k1=1.2, b=0.75):
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
                
    def lda_search(self, measure='tf_idf', scoring='simple_scoring', intersect=False, display=10):
        """Searcher function"""
        self.intersect = False
        self.words = [words.decode('utf-8') for words in self.words]
        if self.words != []:
            lda_query = self.match_topic()
            if lda_query != None:
                for word in self.words[:1]:  # temporary slice, to offer it as an option?
                    lda_query[word] = sum([lda_query[term] for term in lda_query])
                print lda_query
                self.num_hits = {}
                for other_word, freq in lda_query.iteritems():
                    hits = self.get_hits(other_word)
                    results = self.lda_scoring(hits, scoring, freq, measure)
                self.results = dict([(obj_id, self.results[obj_id] * self.num_hits[obj_id]) for obj_id in self.results if self.num_hits[obj_id] > 1])
                return sorted(self.results.iteritems(), key=itemgetter(1), reverse=True)[:display]
            else:
                return []
        else:
            return []
            
    def match_topic(self):
        topic_id = int
        cursor = sqlite_conn(self.path + 'lda_topics.sqlite')
        if len(self.words) == 1:
            cursor.execute('select topic, position from word_position where word=? order by position', (self.words[0],))
            try:
                topic_id = cursor.fetchone()[0]
            except TypeError:
                return None
        else:
            topic_pos = {}
            topic_matches = {}
            query = 'select topic, position from word_position where word="%s"' % self.words[0]
            for word in self.words[1:]:
                query += ' or word="%s"' % word
            cursor.execute(query)
            for topic, position in cursor.fetchall():
                if topic not in topic_pos:
                    topic_pos[topic] = position
                    topic_matches[topic] = 1
                else:
                    topic_pos[topic] += position
                    topic_matches[topic] += 1
            word_num = len(self.words)
            topics = [(topic, topic_pos[topic]) for topic in topic_pos if topic_matches[topic] == word_num]
            if topics == []:
                topics = [(topic, topic_pos[topic]) for topic in topic_pos if topic_matches[topic] == word_num - 1]
            topic_id = sorted(topics, key=itemgetter(1))[0][0]
        cursor.execute('select words from topics where topic=?', (topic_id,))
        results = json.loads(cursor.fetchone()[0])
        topic = [(term, float(freq)) for term, freq in results.iteritems()]# if float(freq) > 0.01]
        topic = dict(sorted(topic, key=itemgetter(1), reverse=True)[:10])
        return topic
        
    def lda_scoring(self, hits, scoring, freq, measure):
        if measure == 'tf_idf':
            idf = self.get_idf(hits)
            for obj_id, word_freq, word_sum in hits:
                tf = float(word_freq) / float(word_sum)
                score = tf * idf * freq
                if obj_id not in self.results:
                    self.results[obj_id] = score
                    self.num_hits[obj_id] = 1
                else:
                    self.results[obj_id] += score    
                    self.num_hits[obj_id] += 1
        else:
            idf = self.get_idf(hits)
            avg_dl = avg_doc_length(self.path)
            k1 = 1.2
            b = 0.75
            for obj_id, word_freq, obj_length in hits:
                tf = float(word_freq)
                dl = float(obj_length)
                temp_score = tf * (k1 + 1.0)
                temp_score2 = tf + k1 * ((1.0 - b) + b * floor(dl / avg_dl))
                score = idf * temp_score / temp_score2 * freq
                if obj_id not in self.results:
                    self.results[obj_id] = score
                    self.num_hits[obj_id] = 1
                else:
                    self.results[obj_id] += score    
                    self.num_hits[obj_id] += 1
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    