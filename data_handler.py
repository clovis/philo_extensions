#!/usr/bin/env python

import numpy as np
import sqlite3
import re
import philologic.PhiloDB
from os import listdir


def np_array_loader(doc_id, path, doc=True, normalize=True, top=0, lower=-1):
    if doc:
        np_array = np.load(path + 'doc_arrays/' + str(doc_id) + '.npy')
    else:
        np_array = np.load(path + 'obj_arrays/' + str(doc_id) + '.npy')
    if normalize == True:
        return np_array[top:lower]/np_array[top:lower].sum()
    else:
        return np_array[top:lower]
          
def sqlite_conn(path):
    conn = sqlite3.connect(path + 'hits_per_word.sqlite')
    return conn.cursor()

def doc_enumerator(path, doc=True):
    if doc:
        obj = re.compile('-')
        suffix = re.compile('(\d+).+')
        return [int(suffix.sub('\\1', doc)) for doc in listdir(path + 'doc_arrays/') if not obj.search(doc)]
    else:
        suffix = re.compile('\.npy')
        doc_num = [suffix.sub('', doc) for doc in listdir(path + 'obj_arrays/')]
    
def doc_counter(path, doc=True):
    if doc:
        return float(len(listdir(path + 'doc_arrays/')))
    else:
        return float(len(listdir(path + 'obj_arrays/')))
    
def words_in_doc(path, doc_id):
    db = philologic.PhiloDB.PhiloDB(path,7)
    filename = db.toms[doc_id]["filename"] + '.count'
    doc = path + 'WORK/' + filename
    return int(open(doc).readline().rstrip())
    
def uniq_words_in_doc(path, doc_id):
    db = philologic.PhiloDB.PhiloDB(path,7)
    filename = db.toms[doc_id]["filename"] + '.words.sorted'
    f = open(path + 'WORK/' + filename)
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.read 
    buf = read_f(buf_size)
    while buf:
        lines += buf.count('\n')
        buf = read_f(buf_size)
    return float(lines)
    
def uniq_words_in_db(path):
    f = open(path  + 'WORK/all.frequencies')
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.read 
    buf = read_f(buf_size)
    while buf:
        lines += buf.count('\n')
        buf = read_f(buf_size)
    return float(lines)
    
def avg_doc_length(path):
    word_count = uniq_words_in_db(path)
    doc_num = doc_counter(path)
    return float(word_count / doc_num)