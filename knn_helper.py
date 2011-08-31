#!/usr/bin/env python

from operator import itemgetter
from data_handler import *
from word_mapper import mapper



class knn(object):
    
    def __init__(self, db, path='/var/lib/philologic/databases/', measure='cosine', db_name=False):
        if db_name:
            db_name = path + db + '/' + db_name
        else:
            db_name = path + db + '/' + measure + '_distance_results.sqlite'
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        
            
    def search(self, obj_id, display=10):
        level = len(obj_id.split())
        while level:
            obj_id = ' '.join(obj_id.split()[:level])
            query = """select neighbor_obj_id, neighbor_distance from obj_results where obj_id = '%s' order by neighbor_distance desc limit %d""" % (obj_id, display)
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            if results != []:
                return results
            level -= 1
        return []