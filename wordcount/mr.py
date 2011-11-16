import re 
import pipeline
import logging
import datetime

from google.appengine.ext import db
from google.appengine.ext import webapp

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

def trans_increment(word, value): 
  def increment(word, value): 
    entity = WCResults.get_by_key_name(word) 
    if not entity: 
      entity = WCResults(key_name=word, value=value, word=word) 
    else: 
      entity.value += value 
    entity.put() 
  db.run_in_transaction(increment, word, value) 

def wordcount_mapper(entity):
  words_dict = {}
  lines = split_into_sentences(entity.lines)
  for ii in lines:
    words = split_into_words(ii)
    for word in words:
      if word in words_dict:
        words_dict[word] += 1
      else:
        words_dict[word] = 1
  for ii in words_dict:
    trans_increment(ii, words_dict[ii])

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
