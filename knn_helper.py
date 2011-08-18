#!/usr/bin/env python

from operator import itemgetter
from data_handler import *
from word_mapper import mapper



class knn(object):
    
    def __init__(self, db, path='/var/lib/philologic/databases/', docs_only=False):
        self.docs_only = docs_only
        db = path + db + '/knn_results.sqlite'
        self.conn = sqlite3.connect(db)
        self.cursor = self.conn.cursor()
        
            
    def search(self, doc_id, display=10):
        if self.docs_only:
            query = 'select neighbor_doc_id, neighbor_distance from doc_results where doc_id = %d order by neighbor_distance desc limit %d' % (doc_id, display)
        else:
            query = """select neighbor_obj_id, neighbor_distance from obj_results where obj_id = '%s' order by neighbor_distance desc limit %d""" % (doc_id, display)
        self.cursor.execute(query)
        self.results = []
        return self.cursor.fetchall()