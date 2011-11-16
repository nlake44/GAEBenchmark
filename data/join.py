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

""" This table should map to the next table """
class TableOne100(db.Model):
  value = db.StringProperty(indexed=False)
 
class TableOne1K(db.Model):
  value = db.StringProperty(indexed=False)

class TableOne10K(db.Model):
  value = db.StringProperty(indexed=False)

class TableOne100K(db.Model):
  value = db.StringProperty(indexed=False)

class TableOne1M(db.Model):
  value = db.StringProperty(indexed=False)

class TableOne10M(db.Model):
  value = db.StringProperty(indexed=False)

""" The other table """
class TableTwo100(db.Model):
  value = db.StringProperty(indexed=False)
 
class TableTwo1K(db.Model):
  value = db.StringProperty(indexed=False)

class TableTwo10K(db.Model):
  value = db.StringProperty(indexed=False)

class TableTwo100K(db.Model):
  value = db.StringProperty(indexed=False)

class TableTwo1M(db.Model):
  value = db.StringProperty(indexed=False)

class TableTwo10M(db.Model):
  value = db.StringProperty(indexed=False)


class JoinResults(db.Model):
  value = db.IntegerProperty(default=0, indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)
  
class TableOut1K(db.Model):
  value1 = db.StringProperty(indexed=False)
  value2 = db.StringProperty(indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)

class TableOut10K(db.Model):
  value1 = db.StringProperty(indexed=False)
  value2 = db.StringProperty(indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)

class TableOut100K(db.Model):
  value1 = db.StringProperty(indexed=False)
  value2 = db.StringProperty(indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)

class TableOut1M(db.Model):
  value1 = db.StringProperty(indexed=False)
  value2 = db.StringProperty(indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)

class TableOut100(db.Model):
  value1 = db.StringProperty(indexed=False)
  value2 = db.StringProperty(indexed=False)
  # Helps to figure out time take to get solution
  created = db.DateTimeProperty(auto_now_add=True, indexed=False)
  modifiedDate = db.DateTimeProperty(auto_now=True, indexed=False)

""" Information of a dataset, such as how long it took to create """
class JoinDataSet(db.Model):
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

def gen_data(num_entries, user, name, entries_per_pipe):
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

  dataset = JoinDataSet(name=name, num_entries=num_entries,
                   entries_per_pipe=entries_per_pipe,
                   user=user, key_name=name, state="Creating")
  dataset.put()

  pipe = TableGenerator(name)
  pipe.start()
  return pipe.base_path + "/status?root=" + pipe.pipeline_id  

def get_class(num_entries):
  if num_entries >= 1000000:
    return "TableOne1M"
  elif num_entries >= 100000:
    return "TableOne100K"
  elif num_entries >= 10000:
    return "TableOne10K"
  elif num_entries >= 1000:
    return "TableOne1K"
  elif num_entries >= 100:
    return "TableOne100"

def get_output_class(num_entries):
  if num_entries >= 1000000:
    return "TableOut1M"
  elif num_entries >= 100000:
    return "TableOut100K"
  elif num_entries >= 10000:
    return "TableOut10K"
  elif num_entries >= 1000:
    return "TableOut1K"
  elif num_entries >= 100:
    return "TableOut100"

def delete_dataset(entity):
  next_key = entity.value
  ent2 = None
  if entity.__class__.__name__ == "TableOne1M":
    ent2 = TableTwo1M.get_by_key_name(next_key)

  if entity.__class__.__name__ == "TableOne100K":
    ent2 = TableTwo100K.get_by_key_name(next_key)

  if entity.__class__.__name__ == "TableOne10K":
    ent2 = TableTwo10K.get_by_key_name(next_key)

  if entity.__class__.__name__ == "TableOne1K":
    ent2 = TableTwo1K.get_by_key_name(next_key)

  if entity.__class__.__name__ == "TableOne100":
    ent2 = TableTwo100.get_by_key_name(next_key)
  ent2.delete()
  entity.delete()

class GenerateData(webapp.RequestHandler):
  def get(self):
    user = users.get_current_user()
    if not user:
      self.redirect(users.create_login_url(dest_url="/"))
      return
    q = JoinDataSet.all()
    q.order('-start')
    results = q.fetch(1000)
    datasets = [result for result in results]
    datasets_len = len(datasets)
    self.response.out.write(template.render("templates/join_data.html",
                                            {"user": user.email(),
                                             "datasets_len" : datasets_len,
                                             "datasets" : datasets}))
  def post(self):
    """ Generate data sets here """
    if self.request.get("generate"):
      num_entries = int(self.request.get("num_entries"))
      user = self.request.get("user")
      name = self.request.get("name")
      epp = int(self.request.get("entries_per_pipe"))
      route = gen_data(num_entries, user, name, epp)
      self.redirect('/data/join')
      # pipeline seems broken
      #self.redirect(route)
    elif self.request.get("delete"):
      name = self.request.get("name") 
      dataset = JoinDataSet.get_by_key_name(name)
      num_entries = dataset.num_entries
      mapreduce_id = control.start_map(
            name="Table removal",
            handler_spec="data.join.delete_dataset",
            reader_spec="mapreduce.input_readers.DatastoreInputReader",
            mapper_parameters={
                "entity_kind": "data.join." + get_class(num_entries),
                "processing_rate": 200
            },
            shard_count=64,
            mapreduce_parameters={model.MapreduceSpec.PARAM_DONE_CALLBACK: 
                       '/data/join/delete_callback'},
            queue_name="default",
          )
      dataset.state = "Deleting" 
      dataset.mr_id = mapreduce_id
      dataset.put()
      self.redirect('/data/join')

class DeleteDoneCallBack(webapp.RequestHandler):
  """ Called after a MR job for deleting a dataset is done"""
  def post(self):
    name = self.request.headers["mapreduce-id"]
    if not name:
      name = "NAME NOT FOUND"
    logging.info("GOT POST CALLBACK " + name)
    q = JoinDataSet.all()
    q.filter('mr_id =', name) 
    q.fetch(1)
    if q:
      for ii in q:
        ii.delete()
        logging.info("deleted: dataset for MR job id %s"%name)
    else:  
      logging.info("Unable to find dataset for MR job id %s"%name)

class TableGenerator(pipeline.Pipeline):
  """A pipeline to generate data
  It will not create exactly num_entries but num_pipes * entries_per_pipe 
  Args:
    id: the id of the dataset entry
    
  """
  def run(self, name):
    dataset = JoinDataSet.get_by_key_name(name)
    num_pipes = dataset.num_entries/dataset.entries_per_pipe
    results = []
    for ii in range(0, num_pipes):
      results.append((yield EntryCreation(ii * dataset.entries_per_pipe,
                          dataset.entries_per_pipe,
                          dataset.num_entries)))
    yield GenDone(name, *results) # Barrier waits

  def finalized(self):
    if not self.was_aborted:
      logging.info("************ Data generator done. **************")

def getNewEntry1(num_entries, keyname, value):
  if num_entries >= 1000000:
    return TableOne1M(key_name=keyname, value=value)
  elif num_entries >= 100000:
    return TableOne100K(key_name=keyname, value=value)
  elif num_entries >= 10000:
    return TableOne10K(key_name=keyname, value=value)
  elif num_entries >= 1000:
    return TableOne1K(key_name=keyname, value=value)
  else:
    return TableOne100(key_name=keyname, value=value)

def getNewEntry2(num_entries, keyname, value):
  if num_entries >= 1000000:
    return TableTwo1M(key_name=keyname, value=value)
  elif num_entries >= 100000:
    return TableTwo100K(key_name=keyname, value=value)
  elif num_entries >= 10000:
    return TableTwo10K(key_name=keyname, value=value)
  elif num_entries >= 1000:
    return TableTwo1K(key_name=keyname, value=value)
  else:
    return TableTwo100(key_name=keyname, value=value)

MAX_ENTRIES_PER_PUT = 500
class EntryCreation(pipeline.Pipeline):
  """ 
  Creates a set of entries for a dataset
  """
  def run(self, start_id, num_create, num_entries):
    entries1 = []
    entries2 = []
    for ii in range(start_id, start_id + num_create):
      entry_key = hashlib.sha1(str(ii)).hexdigest()
      entry_value = hashlib.sha1(str(ii)).hexdigest()
      new_entry1 = getNewEntry1(num_entries, entry_key, entry_value)
      new_entry2 = getNewEntry2(num_entries, entry_value, entry_value)
      entries1.append(new_entry1)
      entries2.append(new_entry2)
      if len(entries1) >= MAX_ENTRIES_PER_PUT:
        db.put(entries1)
        entries1 = []
      if len(entries2) >= MAX_ENTRIES_PER_PUT:
        db.put(entries2)
        entries2 = []
    db.put(entries1)
    db.put(entries2)
    return 1

class GenDone(pipeline.Pipeline):
  """ Called once data has been generated """
  def run(self, name, *results):
    logging.info("GENDONE CALLED")
    num_pipelines = len(results)
    ds = JoinDataSet.get_by_key_name(name)
    then = ds.start
    now = datetime.datetime.now()
    ds.end = now
    delta = (now - then)
    ds.total = delta.days * 86400 + delta.seconds
    ds.state = "Ready"
    ds.put()
    return True

