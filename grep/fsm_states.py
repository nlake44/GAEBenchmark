import logging 
import simplejson as json 
from google.appengine.ext import db
from fantasm import fsm
from fantasm.action import FSMAction
from fantasm.action import DatastoreContinuationFSMAction
from data.grep import GWords1M
from data.grep import GWords100K
from data.grep import GWords10K
from data.grep import GWords1K
from data.grep import GWords100

from data.grep import GrepResults
import re
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

def getQueryKind(num_entries):
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

class StartingStateClass(DatastoreContinuationFSMAction):
  """ Fantasm initial state for wordcount """
  def getQuery(self, context, obj):
    num_entries = int(context['num_entries'])
    return getQueryKind(num_entries) 

  def execute(self, context, obj):
    if not obj['result']:
      return 
    word = obj['result']
    if not word: 
      return
    needle = context['needle']
    if needle in word.lines:
      entity = GrepResults(key_name=word.key().name(), needle=needle, value=word.lines)
      entity.put() 

