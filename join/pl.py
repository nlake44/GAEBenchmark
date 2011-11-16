import re 
import random
import pipeline
import logging
import datetime
import hashlib
from google.appengine.ext import db

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

from records import Record

def getQuery(num_entries):
  if num_entries >= 1000000:
    return TableOne1M.all(keys_only=True)
  if num_entries >= 100000:
    return TableOne100K.all(keys_only=True)
  if num_entries >= 10000:
    return TableOne10K.all(keys_only=True)
  if num_entries >= 1000:
    return TableOne1K.all(keys_only=True)
  if num_entries >= 100:
    return TableOne100.all(keys_only=True)

def createOut(num_entries, keyname, value1, value2):
  if num_entries >= 1000000:
    return TableOut1M(key_name=keyname, value1=value1, value2=value2)
  if num_entries >= 100000:
    return TableOut100K(key_name=keyname, value1=value1, value2=value2)
  if num_entries >= 10000:
    return TableOut10K(key_name=keyname, value1=value1, value2=value2)
  if num_entries >= 1000:
    return TableOut1K(key_name=keyname, value1=value1, value2=value2)
  if num_entries >= 100:
    return TableOut100(key_name=keyname, value1=value1, value2=value2)

def getTable1Ent(num_entries, k):
  if num_entries >= 1000000:
    return TableOne1M.get_by_key_name(k)
  if num_entries >= 100000:
    return TableOne100K.get_by_key_name(k)
  if num_entries >= 10000:
    return TableOne10K.get_by_key_name(k)
  if num_entries >= 1000:
    return TableOne1K.get_by_key_name(k)
  if num_entries >= 100:
    return TableOne100.get_by_key_name(k)

def getTable2Ent(num_entries, k):
  if num_entries >= 1000000:
    return TableTwo1M.get_by_key_name(k)
  if num_entries >= 100000:
    return TableTwo100K.get_by_key_name(k)
  if num_entries >= 10000:
    return TableTwo10K.get_by_key_name(k)
  if num_entries >= 1000:
    return TableTwo1K.get_by_key_name(k)
  if num_entries >= 100:
    return TableTwo100.get_by_key_name(k)

MAX_PER_PUT = 500
class KeyBatch(pipeline.Pipeline):
  def run(self, num, start, end):
    batch = []
    for ii in range(start, end - 1):
      ent1 = getTable1Ent(num, hashlib.sha1(str(ii)).hexdigest())
      if not ent1:
        logging.info("ERROR Ent not found for " + hashlib.sha1(str(ii)).hexdigest() + " number: " + str(ii))
        return
      ent2key = ent1.value
      ent2 = getTable2Ent(num, ent2key) 
      ent3 = createOut(num, ent2key, ent1.value, ent2.value)
      batch.append(ent3)
       
    db.put(batch)
    return True

class JoinPipeline(pipeline.Pipeline):
  def run(self, num_entries):
    batch = []
    start_key = 0
    keys_left = num_entries
    #for ii in range(0, num_entries):
    while True:
      batch.append((start_key, start_key + min(MAX_PER_PUT, keys_left))) 
      start_key = start_key + min(MAX_PER_PUT, keys_left) + 1
      keys_left -= MAX_PER_PUT
      if keys_left <= 0:
        break
    results = []
    for keys in batch:
      start, end = keys 
      results.append( ( yield KeyBatch(num_entries, start, end) ) )
     
  def finalized(self):
    plid = self.pipeline_id
    q = Record.all()
    q.filter('pipeline_id =',plid) 
    items = q.fetch(1)
    for ii in items:
      ii.end = datetime.datetime.now() 
      delta = (ii.end - ii.start)
      logging.info(str(delta.microseconds))
      ii.total = float(delta.days * 86400 + delta.seconds) + float(delta.microseconds)/1000000
      ii.state = "Done"
      ii.put() 
      logging.info("Updated join pipeline record")
    logging.info("Done with join pipeline")
