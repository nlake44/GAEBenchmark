import logging 
import simplejson as json 
from google.appengine.ext import db
from fantasm import fsm
from fantasm.action import FSMAction
from fantasm.action import DatastoreContinuationFSMAction
from data.wordcount import Words1M
from data.wordcount import Words100K
from data.wordcount import Words10K
from data.wordcount import Words1K
from data.wordcount import Words100

from data.wordcount import WordCount100
from data.wordcount import WordCount1K
from data.wordcount import WordCount10K
from data.wordcount import WordCount100K
from data.wordcount import WordCount1M
from data.wordcount import WCResults
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

def trans_increment(word, value):
  def increment(word, value):
    entity = WCResults.get_by_key_name(word)
    if not entity:
      entity = WCResults(key_name=word, value=value, word=word)
    else:
      entity.value += value
    entity.put()
  db.run_in_transaction(increment, word, value)
  #increment(word, value)

def get_word_dict(entity_words):
  words_dict = {}
  lines = split_into_sentences(entity_words)
  for ii in lines:
    words = split_into_words(ii)
    for word in words:
      if word in words_dict:
        words_dict[word] += 1
      else:
        words_dict[word] = 1
  return words_dict

def getQueryKind(num_entries):
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
    diction = get_word_dict(word.lines)
    for ii in diction:
      trans_increment(ii, diction[ii])

if False:      
        # This method takes 4x longer
	class StartingStateClass(DatastoreContinuationFSMAction):
	  """ Fantasm initial state for wordcount """
	  def getQuery(self, context, obj):
	    num_entries = int(context['num_entries'])
	    return getQueryKind(num_entries) 

	  def execute(self, context, obj):
	    if not obj['result']:
	      return 'perentry'
	    word = obj['result']
	    if word:
	      context['dict'] = json.dumps(get_word_dict(word.lines))
	    return 'perentry'
	   
	class MapperStateClass(FSMAction):
	  """ Fantasm mapper class for each word entry"""
	  def execute(self, contexts, obj):
	    diction = {}
	    # Merge all the dictionaries
	    for index, ii, in enumerate(contexts):
	      if 'dict' not in ii:
		continue
	      words = json.loads(ii['dict'])
	      for ii in words:
		if ii in diction:
		  diction[ii] += words[ii]
		else:
		  diction[ii] = words[ii]
	    # transactionally update all words 
	    for ii in diction:
	      trans_increment(ii, diction[ii])

