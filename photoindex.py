#!/usr/bin/python

import pHash
from pHash import ph_dct_imagehash as imagehash

import os,sys, threading
import shelve
from shelve import Shelf
from Queue import Queue

dbfile = "/tmp/photohash.db"

class Spool(threading.Thread):

  def __init__(self, queue, dbfile):
	 self.q = queue
	 self.dbfile = dbfile
	 threading.Thread.__init__(self)

  def run(self):
	 try:
		#open shelf
		db = shelve.open(self.dbfile)

		while True:
		  item = self.q.get()
		  for item_h  in item:
			 print "received item ", item_h
			 item_p = item[item_h]
			 item_h = str(item_h)  
			 if item_h in db:
				print "duplicate foto %s of %s" %(item_p, db[item_h])
			 else:
				db[item_h] = item_p
	 except Exception, e:
		raise e
	 finally:
		#close shelf
		db.sync()
		db.close()


def index_photo(path, q):
  """ traverse a path emitting a couple {hash: path } to the queue"""
  try:
	 for root, dirs, files in os.walk(path):
		for f in files:
		  if f.lower().endswith("jpg") or f.lower().endswith("jpeg") or  f.lower().endswith("png"):
			 fpath = os.path.join(root,f)
			 print "hashing: ", fpath
			 assert os.path.isfile(fpath)
			 tmp, myhash = imagehash(fpath)
			 q.put({myhash: fpath})
  except Exception, e:
	 raise e

def main(path):
  db_queue = Queue()
  spool_t = Spool(db_queue, dbfile)
  spool_t.start()
  index_photo(path, db_queue)
  
  
if __name__ == "__main__":
  main(sys.argv[1])