import re 
import pipeline
import logging
import datetime
from google.appengine.ext import db
from data.wordcount import WCResults
from data.wordcount import Words100
from data.wordcount import Words1K
from data.wordcount import Words10K
from data.wordcount import Words100K
from data.wordcount import Words1M
from data.wordcount import WCDataSet
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

def words_to_diction(words):
  diction = {}
  for word in words:
    lines = split_into_sentences(word.lines)
    for line in lines:
      words = split_into_words(line)
      for word in words:
        if word in diction:
          diction[word] += 1
        else:
          diction[word] = 1
  return diction

class CountEntryPipeline(pipeline.Pipeline):
  def run(self, diction):
    for ii in diction:
      yield TransIncrementPipeline(ii, diction[ii])

class TransIncrementPipeline(pipeline.Pipeline):
  def run(self, word, value):
    def increment(word, value):
      entity = WCResults.get_by_key_name(word)
      if not entity:
        entity = WCResults(key_name=word, value=value, word=word)
      else:
        entity.value += value
      entity.put()
    db.run_in_transaction(increment, word, value)

def getQuery(num_entries):
  if num_entries >= 1000000:
    return Words1M.all()
  if num_entries >= 100000:
    return Words100K.all()
  if num_entries >= 10000:
    return Words10K.all()
  if num_entries >= 1000:
    return Words1K.all()
  if num_entries >= 100:
    return Words100.all()

class LoggerPipe(pipeline.Pipeline):
  def run(self, s):
    logging.info(s)
    return

class WordCountPipelineLoop(pipeline.Pipeline):
  def run(self, num_entries):
    diction = {}
    cursor = None
    while True:
      query = getQuery(num_entries) 
      query.with_cursor(cursor)
      results = query.fetch(1000)
      if len(results) > 0:
        yield CountEntryPipeline(words_to_diction(results))
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
      logging.info("Updated wordcount pipeline record")
    logging.info("Done with wordcount pipeline")
