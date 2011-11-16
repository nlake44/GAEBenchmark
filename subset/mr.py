import random
import re 
import logging
import datetime

from records import Record
from data.subset import SSMRSimpleCounterShard

from google.appengine.ext import db
from google.appengine.ext import webapp

NUM_SHARDS = 100
def increment(value):
  """Increment the value for a given sharded counter."""
  def txn():
    index = random.randint(0, NUM_SHARDS - 1)
    shard_name = "shard" + str(index)
    counter = SSMRSimpleCounterShard.get_by_key_name(shard_name)
    if counter is None:
      counter = SSMRSimpleCounterShard(key_name=shard_name)
    counter.count += value
    counter.put()
  db.run_in_transaction(txn)

def subset_mapper(entity):
  if entity.is_new:
    increment(entity.value)

class MRCallback(webapp.RequestHandler):
  def post(self):
    name = self.request.headers["mapreduce-id"]
    if not name:
      name = "NAME NOT FOUND"
    logging.info("MR CALLBACK " + name)
    q = Record.all()
    q.filter('mr_id =', name)
    q.fetch(1)
    if q:
      for ii in q:
        ii.end = datetime.datetime.now()
        delta = (ii.end - ii.start)
        ii.total = float(delta.days * 86400 + delta.seconds) + float(delta.microseconds)/1000000
        ii.state = "Done"
        ii.put()
        logging.info("updated: record for MR job id %s"%name)
    else:
      logging.info("Unable to find record for MR job id %s"%name)
