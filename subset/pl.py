import re 
import random
import pipeline
import logging
import datetime
from google.appengine.ext import db
from data.subset import SubSetResults
from data.subset import SubSetNumbers100
from data.subset import SubSetNumbers1K
from data.subset import SubSetNumbers10K
from data.subset import SubSetNumbers100K
from data.subset import SubSetNumbers1M
from data.subset import SubSetDataSet
from data.subset import SSPLSimpleCounterShard
from records import Record

NUM_SHARDS = 100
def increment(value):
  """Increment the value for a given sharded counter."""
  def txn():
    index = random.randint(0, NUM_SHARDS - 1)
    shard_name = "shard" + str(index)
    counter = SSPLSimpleCounterShard.get_by_key_name(shard_name)
    if counter is None:
      counter = SSPLSimpleCounterShard(key_name=shard_name)
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
    return SubSetNumbers1M.all().filter('is_new =', True)
  if num_entries >= 100000:
    return SubSetNumbers100K.all().filter('is_new =', True)
  if num_entries >= 10000:
    return SubSetNumbers10K.all().filter('is_new =', True)
  if num_entries >= 1000:
    return SubSetNumbers1K.all().filter('is_new =', True)
  if num_entries >= 100:
    return SubSetNumbers100.all().filter('is_new =', True)

def get_int_array(entities):
  a = []
  for ii in entities:
    a.append(ii.value)
  return a
 
class SubSetPipeline(pipeline.Pipeline):
  def run(self, num_entries):
    cursor = None
    while True:
      query = getQuery(num_entries) 
      query.with_cursor(cursor)
      results = query.fetch(1000)
      if len(results) > 0:
        yield CountEntryPipeline(get_int_array(results))
      if len(results) < 1000:
        return
      else:
        cursor = query.cursor()

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
      logging.info("Updated subset pipeline record")
    logging.info("Done with subset pipeline")
