#!/usr/bin/env python

from operator import itemgetter
from data_handler import *
from word_mapper import mapper



class knn(object):
    
    def __init__(self, db, path='/var/lib/philologic/databases/', table_name=False):
        if table_name:
            db = path + db + '/' + table_name
        else:
            db = path + db + '/knn_results.sqlite'
        self.conn = sqlite3.connect(db)
        self.cursor = self.conn.cursor()
        
            
    def search(self, obj_id, display=10):
        level = len(obj_id.split())
        while level > 1:
            obj_id = ' '.join(obj_id.split()[:level])
            query = """select neighbor_obj_id, neighbor_distance from obj_results where obj_id = '%s' order by neighbor_distance desc limit %d""" % (obj_id, display)
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            if results != []:
                return results
            level -= 1
        return []