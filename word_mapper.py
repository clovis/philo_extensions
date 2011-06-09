#!/usr/bin/env python

import subprocess
import re

class mapper(object):
    
    def __init__(self, path):
        self.file = path  + 'WORK/all.frequencies'
        
    def __getitem__(self, key):
        if type(key) is int:
            return self._id_grep(key)
        return self._grep(key)
        
    def __iter__(self):
        word_id = -1
        for line in open(self.file):
            word = line.strip().split()[1]
            word_id += 1
            yield word, word_id
        
    def _grep(self, word_key):
        pattern = re.compile(word_key + '\W')
        word_id = 0
        for line in open(self.file):
            if pattern.search(line):
                return word_id
            word_id += 1
                
    def _id_grep(self, id_key):
        word_id = 0
        for line in open(self.file):
            if word_id == id_key:
              return line.strip().split()[1]  
            word_id += 1
            
    def id_and_freq(self, word):
        pattern = re.compile('\W' + word + '\W')
        word_id = 0
        for line in open(self.file):
            if pattern.search(line):
                fields = line.split()
                freq = float(line.split()[0])
                return (word_id, freq)
            word_id += 1
            
    ## Version with GNU grep
    def sys_grep(self, word_key):
        process = subprocess.Popen(['grep', '-wn', word, self.file], stdout=subprocess.PIPE)
        match, stderr = process.communicate()
        match = match.strip().split()[0].replace(':', '')
        return int(match) - 1
