import logging 
import simplejson as json 
import random
from google.appengine.ext import db
from fantasm import fsm
from fantasm.action import FSMAction
from fantasm.action import DatastoreContinuationFSMAction
from data.aggregate import Numbers1M
from data.aggregate import Numbers100K
from data.aggregate import Numbers10K
from data.aggregate import Numbers1K
from data.aggregate import Numbers100
from data.aggregate import AggResults
from data.aggregate import FSMSimpleCounterShard
import re

NUM_SHARDS = 100
def increment(value): 
  """Increment the value for a given sharded counter.""" 
  def txn(): 
    index = random.randint(0, NUM_SHARDS - 1) 
    shard_name = "shard" + str(index) 
    counter = FSMSimpleCounterShard.get_by_key_name(shard_name) 
    if counter is None: 
      counter = FSMSimpleCounterShard(key_name=shard_name) 
    counter.count += value
    counter.put() 
  db.run_in_transaction(txn) 

def getQueryKind(num_entries):
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

# This fsm would take 4x longer in SDK
class StartingStateClass2(DatastoreContinuationFSMAction):
  """ Fantasm initial state for aggregate that uses fan in"""
  def getQuery(self, context, obj):
    num_entries = int(context['num_entries'])
    return getQueryKind(num_entries)

  def execute(self, context, obj):
    if not obj['result']:
      return 'perentry'
    word = obj['result']
    if word:
      context['value'] = word.value
    return 'perentry'
	   
class MapperStateClass2(FSMAction):
  """ Fantasm mapper class for each word entry"""
  def execute(self, contexts, obj):
    total = 0
    # Merge all the dictionaries
    for index, ii, in enumerate(contexts):
      if 'value' not in ii:
	continue
      total += int(ii['value'])
      # transactionally update all words 
    if total != 0:
      increment(total)

class StartingStateClass(DatastoreContinuationFSMAction):
  """ Fantasm initial state for aggregate (no fan in used)"""
  def getQuery(self, context, obj):
    num_entries = int(context['num_entries'])
    return getQueryKind(num_entries)

  def execute(self, context, obj):
    word = obj['result']
    if word: increment(word.value)
    return 

