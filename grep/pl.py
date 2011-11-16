import re 
import pipeline
import logging
import datetime
from google.appengine.ext import db
from data.grep import GrepResults
from data.grep import GWords100
from data.grep import GWords1K
from data.grep import GWords10K
from data.grep import GWords100K
from data.grep import GWords1M
from data.grep import GrepDataSet
from records import Record
def split_into_sentences(s):
  """Split text into list of sentences."""
  s = re.sub(r"\s+", " ", s)
  s = re.sub(r"[\\.\\?\\!]", "\n", s)
  return s.split("\n")

def split_into_words(s):
  """Split a sentence into list of words."""
  s = re.sub(r"\W+", " ", s)
  s = re.sub(r"[_0-9]+", " ", s)
  return s.split()

def gwords_to_diction(words, needle):
  diction = {}
  for word in words:
    if needle in word.lines:
      diction[word.key().name()] = word.lines
  return diction

class EntryPipeline(pipeline.Pipeline):
  def run(self, diction, needle):
    results = []
    for ii in diction:
      output = GrepResults(key_name=ii, value=diction[ii], needle=needle) 
      results.append(output)
    db.put(results) 

def getQuery(num_entries):
  if num_entries >= 1000000:
    return GWords1M.all()
  if num_entries >= 100000:
    return GWords100K.all()
  if num_entries >= 10000:
    return GWords10K.all()
  if num_entries >= 1000:
    return GWords1K.all()
  if num_entries >= 100:
    return GWords100.all()

class GrepPipelineLoop(pipeline.Pipeline):
  def run(self, num_entries, needle):
    diction = {}
    cursor = None
    while True:
      query = getQuery(num_entries) 
      query.with_cursor(cursor)
      results = query.fetch(1000)
      if len(results) > 0:
        yield EntryPipeline(gwords_to_diction(results, needle), needle)
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
      logging.info("Updated grep pipeline record")
    logging.info("Done with grep pipeline")
