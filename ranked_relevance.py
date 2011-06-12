#!/usr/bin/env python

import philologic.PhiloDB
import time
import re
from math import log, floor
from operator import itemgetter
from word_mapper import mapper
from data_handler import *


class Searcher(object):
    
    def __init__(self, query, db, path='/var/lib/philologic/databases/'):
        self.path = path + db + '/'
        self.words = self.word_to_id(query)
        self.results = {}        
        
    def get_hits(self, word):
        cursor = sqlite_conn(self.path)
        cursor.execute('select docs from hits where word=?', (word,))
        return [hit.split(',') for hit in cursor.fetchone()[0].split('/')[1:]]
        
    def word_to_id(self, query):
        m = mapper(self.path)
        words = []
        for word in query.split():
            word_info = m.id_and_freq(word)
            if word_info != None:
                words.append(word_info)
        return words
        
    def id_to_word(self, id):
        m = mapper(self.path)
        return m[id]
        
    def get_idf(self, hits):
        total_docs = doc_counter(self.path)
        return log(float(total_docs) / float(len(hits))) + 1
               
    def search(self, measure='tf_idf', scoring='simple_scoring', display=10):
        if self.words != []:
            for word in self.words:
                term, term_freq = word
                hits = self.get_hits(term)
                getattr(self, measure)(term_freq, hits, scoring)
            return sorted(self.results.iteritems(), key=itemgetter(1), reverse=True)[:display]
        else:
            return []
    
    def tf_idf(self, term_freq, hits, scoring):
        idf = self.get_idf(hits)
        for doc, word_freq, word_sum in hits:
            tf = float(word_freq) / float(word_sum)
            score = tf * idf
            getattr(self, scoring)(int(doc), score)
                    
    def frequency(self, term_freq, hits, scoring):
        for doc, word_freq, word_sum in hits:
            score = float(word_freq) / float(word_sum)
            getattr(self, scoring)(int(doc), score)
                    
    def bm25(self, term_freq, hits, scoring, k1=1.2, b=0.75):
        ## a floor is applied to normalized length of doc
        ## in order to diminish the importance of small docs
        ## see http://xapian.org/docs/bm25.html
        idf = self.get_idf(hits)
        avg_dl = avg_doc_length(self.path)
        for doc, word_freq, doc_length in hits:
            tf = float(word_freq)
            dl = float(doc_length)
            temp_score = tf * (k1 + 1.0)
            temp_score2 = tf + k1 * ((1.0 - b) + b * floor(dl / avg_dl))
            score = idf * temp_score / temp_score2
            getattr(self, scoring)(int(doc), score)
                    
    def simple_scoring(self, doc_id, score):
        if doc_id not in self.results:
            self.results[doc_id] = score
        else:
            self.results[doc_id] += score
    
    def dismax_scoring(self, doc_id, score):
        if doc_id not in self.results:
            self.results[doc_id] = score
        else:
            if score > self.results[doc_id]:
                self.results[doc_id] = score
                
    
class Doc_info(object):
    
    def __init__(self, db, query=None, path='/var/lib/philologic/databases/'):
        self.db_path = path + db
        self.db = philologic.PhiloDB.PhiloDB(self.db_path,7)
        
        if query:
            self.query = query.split()
            self.patterns = [re.compile('(?iu)(\A|\W)(%s)(\W)' % word) for word in self.query]
            self.cut_begin = re.compile('\A[^ ]* ')
            self.cut_end = re.compile('<*[^ ]* [^ ]*\Z')
            self.word = 0
            self.philo_search()
            
    def philo_search(self):
        self.hitlist = self.db.query(self.query[self.word])
        time.sleep(.05)
        self.hitlist.update()
        
    def filename(self, doc_id):
        return self.db.toms[doc_id]["filename"]
        
    def title(self, doc_id):
        return self.db.toms[doc_id]['title']
        
    def author(self, doc_id):    
        return self.db.toms[doc_id]["author"]
        
    def get_excerpt(self, doc_id, highlight=False):
        index = self.binary_search(doc_id)
        if index:
            offsets = self.hitlist.get_bytes(self.hitlist[index])
            byte_offset = offsets[0]
            conc_start = byte_offset - 200
            if conc_start < 0:
                conc_start = 0
            text_path = self.db_path + "/TEXT/" + self.filename(doc_id)
            text_file = open(text_path)
            text_file.seek(conc_start)
            text = text_file.read(400)
            if highlight:
                for word in self.patterns:
                    text = word.sub('\\1<span style="color: red">\\2</span>\\3', text)
            text = self.cut_begin.sub('', text)
            text = self.cut_end.sub('', text)
            text = text.replace('<s/>', '')
            return text
        else:
            if self.query[self.word] != self.query[-1]:
                self.word += 1
            else:
                self.word = 0
            self.philo_search()
            self.get_excerpt(doc_id)
        
    def binary_search(self, doc_id, lo=0, hi=None):
        if hi is None:
            hi = len(self.hitlist)
        while lo < hi:
            mid = (lo + hi) // 2
            midval = self.hitlist[mid][0]
            if midval < doc_id:
                lo = mid + 1
            elif midval > doc_id: 
                hi = mid
            else:
                return mid
        return None