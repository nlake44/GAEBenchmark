import logging 
import simplejson as json 
import random
from google.appengine.ext import db
from fantasm import fsm
from fantasm.action import FSMAction
from fantasm.action import DatastoreContinuationFSMAction
from data.subset import SubSetNumbers1M
from data.subset import SubSetNumbers100K
from data.subset import SubSetNumbers10K
from data.subset import SubSetNumbers1K
from data.subset import SubSetNumbers100
from data.subset import SubSetResults
from data.subset import SSFSMSimpleCounterShard
import re

NUM_SHARDS = 100
def increment(value): 
  """Increment the value for a given sharded counter.""" 
  def txn(): 
    index = random.randint(0, NUM_SHARDS - 1) 
    shard_name = "shard" + str(index) 
    counter = SSFSMSimpleCounterShard.get_by_key_name(shard_name) 
    if counter is None: 
      counter = SSFSMSimpleCounterShard(key_name=shard_name) 
    counter.count += value
    counter.put() 
  db.run_in_transaction(txn) 

def getQueryKind(num_entries):
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

class StartingStateClass(DatastoreContinuationFSMAction):
  """ Fantasm initial state for subset """
  def getQuery(self, context, obj):
    num_entries = int(context['num_entries'])
    return getQueryKind(num_entries)

  def execute(self, context, obj):
    word = obj['result']
    if word: increment(word.value)
    return 

