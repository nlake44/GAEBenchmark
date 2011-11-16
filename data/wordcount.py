import pipeline # for creating
import logging
import hashlib
import random
import datetime
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util
from google.appengine.ext.webapp import template
from google.appengine.api import users
#from mapreduce import operation as op
from mapreduce import control
from mapreduce import model
""" These words make up a dataset """
class Words100(db.Model):
  lines = db.TextProperty(default="")
 
class Words1K(db.Model):
  lines = db.TextProperty(default="")

class Words10K(db.Model):
  lines = db.TextProperty(default="")

class Words100K(db.Model):
  lines = db.TextProperty(default="")

class Words1M(db.Model):
  lines = db.TextProperty(default="")

class Words10M(db.Model):
  lines = db.TextProperty(default="")

""" These entries are a part of a run of WC """
class WCResults(db.Model):
  value = db.IntegerProperty(default=0, indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)
  
# The word is the key
class WordCount1K(db.Model):
  value = db.IntegerProperty(default=0, indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)

# The word is the key
class WordCount10K(db.Model):
  value = db.IntegerProperty(default=0, indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)

# The word is the key
class WordCount100K(db.Model):
  value = db.IntegerProperty(default=0, indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)

# The word is the key
class WordCount1M(db.Model):
  value = db.IntegerProperty(default=0, indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)

# The word is the key
class WordCount100(db.Model):
  value = db.IntegerProperty(default=0, indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)

def get_result_query(num_entries):
  if num_entries >= 1000000:
    return WordCount1M.all()
  elif num_entries >= 100000:
    return WordCount100K.all()
  elif num_entries >= 10000:
    return WordCount10K.all()
  elif num_entries >= 1000:
    return WordCount1K.all()
  elif num_entries >= 100:
    return WordCount100.all()

""" Information of a dataset, such as how long it took to create """
class WCDataSet(db.Model):
  link = db.StringProperty(indexed=False)
  start = db.DateTimeProperty(auto_now_add=True)
  end = db.DateTimeProperty()
  total = db.IntegerProperty()
  num_shards = db.IntegerProperty()
  name = db.StringProperty()
  user = db.StringProperty()
  num_entries = db.IntegerProperty()
  entries_per_pipe = db.IntegerProperty()
  char_per_word = db.IntegerProperty()
  state = db.StringProperty()
  mr_id = db.StringProperty() # for deletion
  pipeline_id = db.StringProperty()

def gen_data(num_entries, user, name, char_per_word, entries_per_pipe):
  if num_entries >= 1000000:
    num_entries = 1000000
  elif num_entries >= 100000:
    num_entries = 100000
  elif num_entries >= 10000:
    num_entries = 10000
  elif num_entries >= 1000:
    num_entries = 1000
  elif num_entries >= 100:
    num_entries = 100

  dataset = WCDataSet(name=name, num_entries=num_entries,
                   entries_per_pipe=entries_per_pipe,
                   char_per_word=char_per_word,
                   user=user, key_name=name, state="Creating")
  dataset.put()

  pipe = WordGenerator(name)
  pipe.start()
  return pipe.base_path + "/status?root=" + pipe.pipeline_id  

def get_word_class(num_entries):
  if num_entries >= 1000000:
    return "Words1M"
  elif num_entries >= 100000:
    return "Words100K"
  elif num_entries >= 10000:
    return "Words10K"
  elif num_entries >= 1000:
    return "Words1K"
  elif num_entries >= 100:
    return "Words100"


def delete_dataset(entity):
  entity.delete()

class GenerateData(webapp.RequestHandler):
  def get(self):
    user = users.get_current_user()
    if not user:
      self.redirect(users.create_login_url(dest_url="/"))
      return
    q = WCDataSet.all()
    q.order('-start')
    results = q.fetch(1000)
    datasets = [result for result in results]
    datasets_len = len(datasets)
    self.response.out.write(template.render("templates/wc_data.html",
                                            {"user": user.email(),
                                             "datasets_len" : datasets_len,
                                             "datasets" : datasets}))
  def post(self):
    """ Generate data sets here """
    if self.request.get("generate"):
      # For SDK only generate 1k and less 
      num_entries = int(self.request.get("num_entries"))
      user = self.request.get("user")
      name = self.request.get("name")
      char_per_word = int(self.request.get("char_per_word"))
      entries_pp = int(self.request.get("entries_per_pipe"))
      route = gen_data(num_entries, user, name, char_per_word, entries_pp) 
      self.redirect('/data/wc')
      # pipeline seems broken
      #self.redirect(route)
    elif self.request.get("delete"):
      name = self.request.get("name") 
      dataset = WCDataSet.get_by_key_name(name)
      num_entries = dataset.num_entries
      mapreduce_id = control.start_map(
            name="Word removal",
            handler_spec="data.wordcount.delete_dataset",
            reader_spec="mapreduce.input_readers.DatastoreInputReader",
            mapper_parameters={
                "entity_kind": "data.wordcount." + get_word_class(num_entries),
                "processing_rate": 200
            },
            shard_count=64,
            mapreduce_parameters={model.MapreduceSpec.PARAM_DONE_CALLBACK: 
                       '/data/wc/delete_callback'},
            queue_name="default",
          )
      dataset.state = "Deleting" 
      dataset.mr_id = mapreduce_id
      dataset.put()
      self.redirect('/data/wc')

class DeleteDoneCallBack(webapp.RequestHandler):
  """ Called after a MR job for deleting a dataset is done"""
  def post(self):
    name = self.request.headers["mapreduce-id"]
    if not name:
      name = "NAME NOT FOUND"
    logging.info("GOT POST CALLBACK " + name)
    q = WCDataSet.all()
    q.filter('mr_id =', name) 
    q.fetch(1)
    if q:
      for ii in q:
        ii.delete()
        logging.info("deleted: dataset for MR job id %s"%name)
    else:  
      logging.info("Unable to find dataset for MR job id %s"%name)

class WordGenerator(pipeline.Pipeline):
  """A pipeline to generate data
  It will not create exactly num_entries but num_pipes * entries_per_pipe 
  Args:
    id: the id of the dataset entry
    
  """
  def run(self, name):
    dataset = WCDataSet.get_by_key_name(name)
    num_pipes = dataset.num_entries/dataset.entries_per_pipe
    results = []
    for ii in range(0, num_pipes):
      results.append((yield EntryCreation(ii * dataset.entries_per_pipe,
                          dataset.entries_per_pipe,
                          dataset.char_per_word,
                          dataset.num_entries)))
    yield GenDone(name, *results) # Barrier waits

  def finalized(self):
    if not self.was_aborted:
      logging.info("************ Data generator done. **************")

def getNewEntry(num_entries, keyname, value):
  if num_entries >= 1000000:
    return Words1M(key_name=keyname, lines=value)
  elif num_entries >= 100000:
    return Words100K(key_name=keyname, lines=value)
  elif num_entries >= 10000:
    return Words10K(key_name=keyname, lines=value)
  elif num_entries >= 1000:
    return Words1K(key_name=keyname, lines=value)
  else:
    return Words100(key_name=keyname, lines=value)

DICT = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
def get_word(chars_per_word):
  """ 
  Generates a random word of a certain length
  """
  word = ""
  for ii in range(0, chars_per_word):
    word += random.choice(DICT)
  return word

def gen_line(chars_per_word, num_words=1):
  """
  Creates a line of words separated by " " 
  """
  line = ""
  for ii in range(0, num_words):
    line += get_word(chars_per_word)
    line += " "
  return line

MAX_ENTRIES_PER_PUT = 500
class EntryCreation(pipeline.Pipeline):
  """ 
  Creates a set of entries for a dataset
  """
  def run(self, start_id, num_create, char_per_word, num_entries):
    entries = []
    for ii in range(start_id, start_id + num_create):
      entry_key = hashlib.sha1(str(ii)).hexdigest()
      entry_value = gen_line(char_per_word)
      new_entry = getNewEntry(num_entries, entry_key, entry_value)
      entries.append(new_entry)
      if len(entries) >= MAX_ENTRIES_PER_PUT:
        db.put(entries)
        entries = []
    db.put(entries)
    return 1

class GenDone(pipeline.Pipeline):
  """ Called once data has been generated """
  def run(self, name, *results):
    logging.info("GENDONE CALLED")
    num_pipelines = len(results)
    ds = WCDataSet.get_by_key_name(name)
    then = ds.start
    now = datetime.datetime.now()
    ds.end = now
    delta = (now - then)
    ds.total = delta.days * 86400 + delta.seconds
    ds.state = "Ready"
    ds.put()
    return True

