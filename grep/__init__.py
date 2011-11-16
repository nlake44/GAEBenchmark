import logging
from data.grep import GrepDataSet
from data.grep import get_result_query
from data.grep import GrepResults
from records import Record
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util
from google.appengine.ext.webapp import template
from google.appengine.api import users
from fantasm import fsm
from fantasm.action import FSMAction
from fantasm.action import DatastoreContinuationFSMAction
from grep.pl import GrepPipelineLoop

from mapreduce import model
from mapreduce import control 

def getKindString(num_entries):
  if num_entries >= 1000000:
    return "GWords1M"
  elif num_entries >= 100000:
    return "GWords100K"
  elif num_entries >= 10000:
    return "GWords10K"
  elif num_entries >= 1000:
    return "GWords1K"
  elif num_entries >= 100:
    return "GWords100"
 

# Wordcount for mr, pipeline, and fsm
class Grep(webapp.RequestHandler):
  def get(self):
    user = users.get_current_user()
    if not user:
      self.redirect(users.create_login_url(dest_url="/"))
      return
    q = GrepDataSet.all()
    q.order('-start')
    results = q.fetch(1000)
    datasets = [result for result in results]
    datasets_len = len(datasets)
    q = Record.all()
    q.filter('benchmark =', "grep")
    q.order('-start')
    results = q.fetch(1000) 
    records = [result for result in results]
    records_len = len(records)
    self.response.out.write(template.render("templates/grep.html",
                                            {"user": user.email(),
                                             "datasets_len" : datasets_len,
                                             "datasets" : datasets,
                                             "records": records,
                                             "records_len" : records_len}))
  def post(self):
    """ Generate data sets here """
    if self.request.get("fsm_cleanup"):
      if fsm_calculate_run_time():
        self.redirect('/grep')
      else:
        self.response.out.write("Error calculating fsm/grep")
      return 
    if self.request.get("compute"):
      engine = self.request.get("engine")
      dataset = self.request.get("dataset")
      user = self.request.get('user')
      needle = self.request.get('needle')    
      data = GrepDataSet.get_by_key_name(dataset)
      record = Record(engine_type=engine, 
                      dataset=dataset,
                      benchmark="grep",
                      num_entities=data.num_entries,
                      #shard_count=data.num_pipelines,
                      entries_per_pipe=data.entries_per_pipe,
                      user=user,
                      char_per_word=data.char_per_word,
                      state="Running")
      if engine == "fsm":
        record.put()
        context = {}
        context['user'] = str(user)
        context['num_entries'] = int(data.num_entries)
        context['needle'] = needle
        fsm.startStateMachine('Grep', [context])
        self.redirect('/grep')
      elif engine == "pipeline":
        mypipeline = GrepPipelineLoop(data.num_entries, needle)
        mypipeline.start()
        record.pipeline_id = mypipeline.pipeline_id
        record.put()
        self.redirect('/grep')
        #self.redirect(mypipeline.base_path + "/status?root=" + mypipeline.pipeline_id)
        return
      elif engine == "mr":
        # Why 1k each per shard or less? is this ideal?
        if data.num_entries > 1000:
          shards = data.num_entries/1000
          shards = min(256, shards) 
        else: shards = 1

        kind = getKindString(data.num_entries)
        mapreduce_id = control.start_map(
            name="Grep",
            handler_spec="grep.mr.grep_mapper",
            reader_spec="mapreduce.input_readers.DatastoreInputReader",
            mapper_parameters={
                "entity_kind": "data.grep."+kind,
                "processing_rate": 500,
                "needle":needle,
            },
            mapreduce_parameters={model.MapreduceSpec.PARAM_DONE_CALLBACK:
                       '/grep/mr/callback'},
            shard_count=shards,
            queue_name="default",
          )

        record.mr_id = mapreduce_id
        record.put()
        self.redirect('/grep')
        

def fsm_calculate_run_time():
  """ Fantasm does not give call backs when its done. Must figure it out
      with another job using the last modified date on output entities
  """
  # Get the last job which was run for grep/fsm
  q = Record.all() 
  q.filter('engine_type =','fsm')
  q.filter('benchmark =','grep')
  q.order('-start')
  results = q.fetch(1)
  if len(results) == 0:
    logging.error("Unable to find a record for fsm/grep")
    return False

  q = None
  record = None
  for ii in results:
    if ii.state == "Done":
      logging.error("Last FSM end time has already been calculated")
    q = GrepResults.all()
    if not q:
      logging.error("No query returned for Grep results")
      return False
    record = ii

  max_date = None 
  while True:
    results = q.fetch(1000)  
    for ii in results:
      date = ii.modifiedDate
      if max_date == None or max_date < date:
        max_date = date 
    if len(results) < 1000:
      break;
  record.state = "Done"
  record.end = max_date
  delta = (record.end - record.start)
  record.total = float(delta.days * 86400 + delta.seconds) + float(delta.microseconds)/1000000
  record.put()
  return True  
