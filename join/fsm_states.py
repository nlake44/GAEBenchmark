import logging 
import simplejson as json 
import random
from google.appengine.ext import db
from fantasm import fsm
from fantasm.action import FSMAction
from fantasm.action import DatastoreContinuationFSMAction
from data.join import TableOne1M
from data.join import TableOne100K
from data.join import TableOne10K
from data.join import TableOne1K
from data.join import TableOne100

from data.join import TableTwo1M
from data.join import TableTwo100K
from data.join import TableTwo10K
from data.join import TableTwo1K
from data.join import TableTwo100

from data.join import TableOut1M
from data.join import TableOut100K
from data.join import TableOut10K
from data.join import TableOut1K
from data.join import TableOut100

def getQueryKind(num_entries):
  if num_entries >= 1000000:
    return TableOne1M.all()
  if num_entries >= 100000:
    return TableOne100K.all()
  if num_entries >= 10000:
    return TableOne10K.all()
  if num_entries >= 1000:
    return TableOne1K.all()
  if num_entries >= 100:
    return TableOne100.all()
      
class StartingStateClass(DatastoreContinuationFSMAction):
  """ Fantasm initial state for join """
  def getQuery(self, context, obj):
    num_entries = int(context['num_entries'])
    return getQueryKind(num_entries)

  def execute(self, context, obj):
    if not obj['result']:
      return
    entry = obj['result']
    num_entries = int(context['num_entries'])
    if not entry:
      return 

    if num_entries >= 1000000:
      entry2 = TableTwo1M.get_by_key_name(entry.value)
      entry3 = TableOut1M(key_name=entry.value, value1=entry.value, value2=entry2.value)
      entry3.put()
    elif num_entries >= 100000:
      entry2 = TableTwo100K.get_by_key_name(entry.value)
      entry3 = TableOut100K(key_name=entry.value, value1=entry.value, value2=entry2.value)
      entry3.put()
    elif num_entries >= 10000:
      entry2 = TableTwo10K.get_by_key_name(entry.value)
      entry3 = TableOut10K(key_name=entry.value, value1=entry.value, value2=entry2.value)
      entry3.put()
    elif num_entries >= 1000:
      entry2 = TableTwo1K.get_by_key_name(entry.value)
      entry3 = TableOut1K(key_name=entry.value, value1=entry.value, value2=entry2.value)
      entry3.put()
    elif num_entries >= 100:
      entry2 = TableTwo100.get_by_key_name(entry.value)
      entry3 = TableOut100(key_name=entry.value, value1=entry.value, value2=entry2.value)
      entry3.put()
