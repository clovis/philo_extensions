#!/usr/bin/env python

from operator import itemgetter
from scipy.spatial.distance import *
from data_handler import *
from word_mapper import mapper



class knn(object):
    
    def __init__(self, doc, path, docs=None, top_words=0, lower_words=-1, metric=cosine):
        self.id_to_word = mapper(path + '/WORK/all.frequencies')
        self.top = top_words
        self.lower = lower_words
        self.orig = self.loader(doc, path, top_words, lower_words)
        self.metric = metric
        self.path = path
        self.results = []
        if docs == None:
            self.docs = doc_enumerator(path)
        else:
            self.docs = docs
            
    def search(self):
        self.__call__()
        
    def __call__(self):
        for doc_id in self.docs:
            doc = np_array_loader(doc_id, self.path, top=self.top, lower=self.lower)
            self.results.append((doc_id, 1 - self.metric(self.orig, doc)))
        
    def loader(self, doc, path, top_words, lower_words):
        return np_array_loader(doc, path, top=top_words, lower=lower_words)
        
    def sort_results(self, order=True, display=10):
        return sorted(self.results, key=itemgetter(1), reverse=order)[:display]
        
    def word_freq_sort(self, doc, path):
        # exclude last column in row since it doesn't exist in word mappings
        mydoc = np_array_loader(doc, path)
        frequencies = [(self.id_to_word[word], mydoc[word]) for word in mydoc[:-1].nonzero()[0]]
        return sorted(frequencies, key=itemgetter(1), reverse=True)[100:110]