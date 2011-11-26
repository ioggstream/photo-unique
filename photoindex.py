#!/usr/bin/python

import pHash
from pHash import ph_dct_imagehash as imagehash

import os,sys, threading
import shelve
from shelve import Shelf
from Queue import Queue

dbfile = "/tmp/photohash.db"

stop_threads = False

class Spool(threading.Thread):

  def __init__(self, queue, dbfile):
	 self.q = queue
	 self.dbfile = dbfile
	 threading.Thread.__init__(self)

  def run(self):
	 print "Starting Spool thread"
	 try:
		#open shelf
		db = shelve.open(self.dbfile)

		while not stop_threads:
		  item = self.q.get()
		  for item_h  in item:
			 print "received item ", item_h
			 item_p = item[item_h]
			 item_h = str(item_h)  
			 if item_h in db:
				print "duplicate foto %s of %s" %(item_p, db[item_h])
			 else:
				db[item_h] = item_p
                print "Exiting from thread: Spool"
	 except Exception, e:
		raise e
	 finally:
		#close shelf
		db.sync()
		db.close()


class PhotoIndexer(threading.Thread):
    """ Calculate the photo hash and send it to db_queue.

        There can be multiple queues, but the db_queue should be
        the same for every thread
    """
    def __init__(self, queue, db_queue):
        self.queue = queue
        self.db_queue = db_queue

	threading.Thread.__init__(self)

    def run(self):
	print "Start PhotoIndexer thread\n" 
        hash_photo(self.queue, self.db_queue)


def is_image(filename):
	if not filename: return False
	filename = filename.lower()
	for ext in ["jpg", "jpeg", "png", "gif"]:
		if filename.endswith(ext): return True
	return False

def hash_photo(path_queue, db_queue):
    """receive an indexing request from a given queue.
	it's the main action for the PhotoIndexer thread
    """
    while not stop_threads:
	path = path_queue.get()
	try:
	    print "Opening file %s"% path
	    trash, myhash = imagehash(path)
	    db_queue.put({myhash:path})
	except Exception, e:
	    print "error %s hashing file %s" % (e, path)

    print "Exiting from thread: Spool"
	    
    
	

def request_index(path, queues):
  """ traverse a path emitting a couple {hash: path } to the queue"""
  try:
	assert queues
	q_no = len(queues)
	i=0
	for root, dirs, files in os.walk(path):
	    for f in files:
		if is_image(f):
		    fpath = os.path.join(root,f)
		    assert os.path.isfile(fpath)
		    queues[i].put(fpath)
		    i = (i+1) % q_no
  except Exception, e:
	print "Error %s" % e
	raise e

def main(path):
  db_queue = Queue()
  hash_queues = [Queue() for i in range(3)]

  spool_t = Spool(db_queue, dbfile)
  spool_t.start()

  for q in hash_queues:
	queue_t = PhotoIndexer(q, db_queue)
	queue_t.start()

  request_index(path, hash_queues)
  stop_threads = True
  
  spool_t.join()
  
  
if __name__ == "__main__":
  sys.exit(main(sys.argv[1]))
