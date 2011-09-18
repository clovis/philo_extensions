#!/usr/bin/env python


from philologic import PhiloDB, SqlToms
import re
import time
import sqlite3
import unicodedata
from difflib import get_close_matches


class DocInfo(object):
    """Helper class meant to provide various information on documents.
    It provides various convenience functions based on the PhiloLogic library"""
    
    def __init__(self, db, query=None, path='/var/lib/philologic/databases/'):
        self.db_path = path + db
        self.toms = SqlToms.SqlToms(self.db_path +'/toms.db', 7)
        
        if query:
            self.query = query.split()
            self.patterns = [re.compile('(?iu)(\A|\W)(%s)(\W)' % word) for word in self.query]
            self.cut_begin = re.compile('\A[^ ]* ')
            self.cut_end = re.compile('<*[^ ]* [^ ]*\Z')
            self.word = 0
            self.philo_search()
            
    def philo_search(self):
        """Query the PhiloLogic database and retrieve a hitlist"""
        db = PhiloDB.PhiloDB(self.db_path,7)
        self.hitlist = db.query(self.query[self.word])
        time.sleep(.05)
        self.hitlist.update()
        
    def get_metadata(self, obj_id, field):
        return self.__get_info(obj_id=obj_id, field=field)
        
    def get_obj_id(self, **metadata_info):
        return self.__get_info(**metadata_info)
        
    def __get_info(self, obj_id=False, field=None, **metadata_info):
        if obj_id:
            try:
                obj_id = obj_id.replace('-', ' ')
                obj_id = tuple(obj_id.split())
                level = len(obj_id)
                info = None
                while level:
                    info = self.toms[obj_id[:level]][field]
                    if isinstance(info, str):
                        break
                    level -= 1
            except AttributeError:
                info = self.toms[obj_id][field]
        else:
            info = [hit['philo_id'] for hit in self.toms.query(**metadata_info)]
            if info == []:
                conn = sqlite3.connect(self.db_path + '/toms.db')
                c = conn.cursor()
                c.execute('select head from toms')
                headword_dict = dict([(headword[0].lower(), headword[0]) for headword in c.fetchall() if headword[0]  != None])
                headword_list = [headword for headword in headword_dict]
                close_matches = [headword_dict[word] for word in get_close_matches(metadata_info['head'], headword_list, 5)]
                for match in close_matches:
                    c.execute("SELECT philo_id FROM toms WHERE head = ?;",(match,))
                    info.append(c.fetchone()[0])
        return info            
        
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
            text_path = self.db_path + "/TEXT/" + self.get_info(doc_id, 'filename')
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