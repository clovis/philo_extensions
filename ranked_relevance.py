#!/usr/bin/env python

import philologic.PhiloDB
import time
import re
from math import log, floor
from operator import itemgetter
from word_mapper import mapper
from data_handler import *


class Searcher(object):
    
    def __init__(self, query, db, doc_level_search=True, path='/var/lib/philologic/databases/'):
        self.path = path + db + '/'
        self.words = self.word_to_id(query)
        self.doc_level_search = doc_level_search
        self.results = {}        
        
    def get_hits(self, word, doc=True):
        cursor = sqlite_conn(self.path)
        if self.doc_level_search:
            cursor.execute('select doc_id, word_freq, total_words from doc_hits where word=?', (word,))
        else:
            cursor.execute('select obj_id, word_freq, total_words from obj_hits where word=?', (word,))
        return cursor.fetchall()
        
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
               
    def search(self, measure='tf_idf', scoring='simple_scoring', intersect=False, display=10):
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
                
    
class Doc_info(object):
    """Helper class meant to provide various information on document.
    It provides various convenience functions based on the PhiloLogic library"""
    
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
        """Query the PhiloLogic database and retrieve a hitlist"""
        self.hitlist = self.db.query(self.query[self.word])
        time.sleep(.05)
        self.hitlist.update()
        
    def __check_id(self, doc_id):
        """Make sure the document id isn't a string. If so, split the object id,
        and return the first element as the document id"""
        if type(doc_id) == int:
            return doc_id
        else:
            return doc_id.split()[0]
        
    def filename(self, doc_id):
        """Return filename given a document id"""
        doc_id = self.__check_id(doc_id)
        return self.db.toms[doc_id]["filename"]
      
    def title(self, doc_id):
        """Return a title given a document id"""
        return self.db.toms[doc_id]['title']
        
    def author(self, doc_id):
        """Return an author given a document id"""
        doc_id = self.__check_id(doc_id)
        return self.db.toms[doc_id]["author"]
        
    def get_excerpt(self, doc_id, highlight=False):
        """Return a text excerpt by querying PhiloLogic and using 
        the byte offset to extract the passage"""
        doc_id = self.__check_id(doc_id)
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
        """Based on the Python bisect module"""
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