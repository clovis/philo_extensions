#!/usr/bin/env python

import numpy as np
import sqlite3
import re
from os import listdir


def np_load(obj_id, path, normalize=True, top=0, lower=None):
    np_array = np.load(path + str(obj_id) + '.npy')
    if normalize == True:
        return np_array[top:lower]/np_array[top:lower].sum()
    else:
        return np_array[top:lower]
          
def sqlite_conn(path):
    conn = sqlite3.connect(path)
    conn.text_factory = str
    return conn.cursor()

def doc_enumerator(path, docs_only=True):
    if docs_only:
        suffix = re.compile('(\d+).+')
        return [int(suffix.sub('\\1', doc)) for doc in listdir(path + 'doc_arrays/')]
    else:
        suffix = re.compile('\.npy')
        return [suffix.sub('', doc) for doc in listdir(path + 'obj_arrays/')]
    
def doc_counter(path):
    return float(len(listdir(path)))
    
def words_in_doc(path, doc_id):
    import philologic.PhiloDB
    db = philologic.PhiloDB.PhiloDB(path,7)
    filename = db.toms[doc_id]["filename"] + '.count'
    doc = path + 'WORK/' + filename
    return int(open(doc).readline().rstrip())
       
def uniq_words_in_db(path):
    return int(open(path + 'word_num.txt').readline().rstrip())
    
def avg_doc_length(path):
    word_count = uniq_words_in_db(path)
    doc_num = doc_counter(path)
    return float(word_count / doc_num)