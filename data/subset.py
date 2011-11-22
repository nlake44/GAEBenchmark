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

class SSFSMSimpleCounterShard(db.Model):
  """Shards for the counter"""
  count = db.IntegerProperty(required=True, default=0, indexed=False)
  modified = db.DateTimeProperty(indexed=False, auto_now=True)

class SSPLSimpleCounterShard(db.Model):
  """Shards for the counter"""
  count = db.IntegerProperty(required=True, default=0, indexed=False)
  modified = db.DateTimeProperty(indexed=False, auto_now=True)

class SSMRSimpleCounterShard(db.Model):
  """Shards for the counter"""
  count = db.IntegerProperty(required=True, default=0, indexed=False)
  modified = db.DateTimeProperty(indexed=False, auto_now=True)

def get_fsm_count():
  total = 0
  for counter in SSFSMSimpleCounterShard.all():
    total += counter.count
  return total

def get_pl_count():
  total = 0
  for counter in SSPLSimpleCounterShard.all():
    total += counter.count
  return total

def get_mr_count():
  total = 0
  for counter in SSMRSimpleCounterShard.all():
    total += counter.count
  return total


""" These numbers make up a dataset """
class SubSetNumbers100(db.Model):
  value = db.IntegerProperty(indexed=False)
  is_new = db.BooleanProperty() 

class SubSetNumbers1K(db.Model):
  value = db.IntegerProperty(indexed=False)
  is_new = db.BooleanProperty() 

class SubSetNumbers10K(db.Model):
  value = db.IntegerProperty(indexed=False)
  is_new = db.BooleanProperty() 

class SubSetNumbers100K(db.Model):
  value = db.IntegerProperty(indexed=False)
  is_new = db.BooleanProperty() 

class SubSetNumbers1M(db.Model):
  value = db.IntegerProperty(indexed=False)
  is_new = db.BooleanProperty() 

class SubSetNumbers10M(db.Model):
  value = db.IntegerProperty(indexed=False)
  is_new = db.BooleanProperty() 

""" These entries are a part of a run of SubSet """
class SubSetResults(db.Model):
  value = db.IntegerProperty(default=0, indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)
 
""" Information of a dataset, such as how long it took to create """
class SubSetDataSet(db.Model):
  link = db.StringProperty(indexed=False)
  start = db.DateTimeProperty(auto_now_add=True)
  end = db.DateTimeProperty()
  total = db.IntegerProperty()
  num_shards = db.IntegerProperty()
  name = db.StringProperty()
  user = db.StringProperty()
  num_entries = db.IntegerProperty()
  entries_per_pipe = db.IntegerProperty()
  state = db.StringProperty()
  mr_id = db.StringProperty() # for deletion
  percent_new = db.IntegerProperty()

def gen_data(num_entries, user, name, entries_per_pipe, percent_new):
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

  dataset = SubSetDataSet(name=name, num_entries=num_entries,
                   entries_per_pipe=entries_per_pipe, percent_new=percent_new,
                   user=user, key_name=name, state="Creating")
  dataset.put()

  pipe = SubSetGenerator(name)
  pipe.start()
  return pipe.base_path + "/status?root=" + pipe.pipeline_id  

def get_class(num_entries):
  if num_entries >= 1000000:
    return "SubSetNumbers1M"
  elif num_entries >= 100000:
    return "SubSetNumbers100K"
  elif num_entries >= 10000:
    return "SubSetNumbers10K"
  elif num_entries >= 1000:
    return "SubSetNumbers1K"
  elif num_entries >= 100:
    return "SubSetNumbers100"

def delete_dataset(entity):
  entity.delete()

class GenerateData(webapp.RequestHandler):
  def get(self):
    user = users.get_current_user()
    if not user:
      self.redirect(users.create_login_url(dest_url="/"))
      return
    q = SubSetDataSet.all()
    q.order('-start')
    results = q.fetch(1000)
    datasets = [result for result in results]
    datasets_len = len(datasets)
    self.response.out.write(template.render("templates/subset_data.html",
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
      percent = int(self.request.get('percent'))
      entries_pp = int(self.request.get("entries_per_pipe"))
      route = gen_data(num_entries, user, name, entries_pp, percent) 
      self.redirect('/data/subset')
      # pipeline seems broken
      #self.redirect(route)
    elif self.request.get("delete"):
      name = self.request.get("name") 
      dataset = SubSetDataSet.get_by_key_name(name)
      num_entries = dataset.num_entries
      mapreduce_id = control.start_map(
            name="Subset entry removal",
            handler_spec="data.wordcount.delete_dataset",
            reader_spec="mapreduce.input_readers.DatastoreInputReader",
            mapper_parameters={
                "entity_kind": "data.subset." + get_class(num_entries),
                "processing_rate": 200
            },
            shard_count=64,
            mapreduce_parameters={model.MapreduceSpec.PARAM_DONE_CALLBACK: 
                       '/data/subset/delete_callback'},
            queue_name="default",
          )
      dataset.state = "Deleting" 
      dataset.mr_id = mapreduce_id
      dataset.put()
      self.redirect('/data/subset')

class DeleteDoneCallBack(webapp.RequestHandler):
  """ Called after a MR job for deleting a dataset is done"""
  def post(self):
    name = self.request.headers["mapreduce-id"]
    if not name:
      name = "NAME NOT FOUND"
    logging.info("GOT POST CALLBACK " + name)
    q = SubSetDataSet.all()
    q.filter('mr_id =', name) 
    q.fetch(1)
    if q:
      for ii in q:
        ii.delete()
        logging.info("deleted: dataset for MR job id %s"%name)
    else:  
      logging.info("Unable to find dataset for MR job id %s"%name)

class SubSetGenerator(pipeline.Pipeline):
  """A pipeline to generate data
  It will not create exactly num_entries but num_pipes * entries_per_pipe 
  Args:
    id: the id of the dataset entry
    
  """
  def run(self, name):
    dataset = SubSetDataSet.get_by_key_name(name)
    num_pipes = dataset.num_entries/dataset.entries_per_pipe
    results = []
    isFirst = True # always have at least one "new" entry
    for ii in range(0, num_pipes):
      results.append((yield EntryCreation(ii * dataset.entries_per_pipe,
                          dataset.entries_per_pipe,
                          dataset.num_entries, dataset.percent_new, isFirst)))
      isFirst = False 
    yield GenDone(name, *results) # Barrier waits

  def finalized(self):
    if not self.was_aborted:
      logging.info("************ Data generator done. **************")

def getNewEntry(num_entries, keyname, value, is_new):
  if num_entries >= 1000000:
    return SubSetNumbers1M(key_name=keyname, value=value, is_new=is_new)
  elif num_entries >= 100000:
    return SubSetNumbers100K(key_name=keyname, value=value, is_new=is_new)
  elif num_entries >= 10000:
    return SubSetNumbers10K(key_name=keyname, value=value, is_new=is_new)
  elif num_entries >= 1000:
    return SubSetNumbers1K(key_name=keyname, value=value, is_new=is_new)
  else:
    return SubSetNumbers100(key_name=keyname, value=value, is_new=is_new)

def get_number():
  """ 
  Generates a random number between 1 and 10 
  """
  return random.randint(1,10)

def isNew(percent):
  """ 
  """ 
  return percent > random.randint(0,100)

MAX_ENTRIES_PER_PUT = 500
class EntryCreation(pipeline.Pipeline):
  """ 
  Creates a set of entries for a dataset
  """
  def run(self, start_id, num_create, num_entries, percent_new, isFirst):
    entries = []
    for ii in range(start_id, start_id + num_create):
      entry_key = hashlib.sha1(str(ii)).hexdigest()

      if isFirst:  # ensure at least one entry is marked for processing
        isnew = True
        isFirst = False
      else: isnew = isNew(percent_new)

      new_entry = getNewEntry(num_entries, entry_key, get_number(), isnew)

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
    ds = SubSetDataSet.get_by_key_name(name)
    then = ds.start
    now = datetime.datetime.now()
    ds.end = now
    delta = (now - then)
    ds.total = delta.days * 86400 + delta.seconds
    ds.state = "Ready"
    ds.put()
    return True

