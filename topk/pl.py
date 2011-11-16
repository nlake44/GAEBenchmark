import re 
import random
import pipeline
import logging
import datetime
from google.appengine.ext import db
from data.aggregate import AggResults
from data.aggregate import Numbers100
from data.aggregate import Numbers1K
from data.aggregate import Numbers10K
from data.aggregate import Numbers100K
from data.aggregate import Numbers1M
from data.aggregate import AggDataSet
from data.aggregate import PLSimpleCounterShard
from records import Record

NUM_SHARDS = 100
def increment(value):
  """Increment the value for a given sharded counter."""
  def txn():
    index = random.randint(0, NUM_SHARDS - 1)
    shard_name = "shard" + str(index)
    counter = PLSimpleCounterShard.get_by_key_name(shard_name)
    if counter is None:
      counter = PLSimpleCounterShard(key_name=shard_name)
    counter.count += value
    counter.put()
  db.run_in_transaction(txn)

class CountEntryPipeline(pipeline.Pipeline):
  def run(self, entities):
    total = 0
    for ii in entities:
      total += ii
    increment(total)

def getQuery(num_entries):
  if num_entries >= 1000000:
    return Numbers1M.all()
  if num_entries >= 100000:
    return Numbers100K.all()
  if num_entries >= 10000:
    return Numbers10K.all()
  if num_entries >= 1000:
    return Numbers1K.all()
  if num_entries >= 100:
    return Numbers100.all()

def get_int_array(entities):
  a = []
  for ii in entities:
    a.append(ii.value)
  return a
 
class AggregatePipeline(pipeline.Pipeline):
  def run(self, num_entries):
    query = getQuery(num_entries) 
    while True:
      results = query.fetch(1000)
      if len(results) > 0:
        yield CountEntryPipeline(get_int_array(results))
      if len(results) < 1000:
        return

  def finalized(self):
    plid = self.pipeline_id
    q = Record.all()
    q.filter('pipeline_id =',plid) 
    items = q.fetch(1)
    for ii in items:
      ii.end = datetime.datetime.now() 
      delta = (ii.end - ii.start)
      ii.total = float(delta.days * 86400 + delta.seconds) + float(delta.microseconds)/1000000
      ii.state = "Done"
      ii.put() 
      logging.info("Updated aggregate pipeline record")
    logging.info("Done with aggregate pipeline")
