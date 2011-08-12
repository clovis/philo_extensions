#!/usr/bin/env python


import philologic.PhiloDB
import re
import time


class DocInfo(object):
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
        
    def get_info(self, obj_id, field):
        try:
            obj_id = obj_id.replace('-', ' ')
            obj_id = tuple(obj_id.split())
            level = len(obj_id)
            metadata = None
            while level > 1:
                metadata = self.db.toms[obj_id[:level]][field]
                level -= 1
        except AttributeError:
            metadata = self.db.toms[obj_id][field]
        return metadata
            
        
    def get_excerpt(self, doc_id, highlight=False):
        """Return a text excerpt by querying PhiloLogic and using 
        the byte offset to extract the passage"""
        doc_id = doc_id.split()[0]
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