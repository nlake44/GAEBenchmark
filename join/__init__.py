import logging
import datetime
from data.join import JoinDataSet
from data.join import get_output_class
from records import Record
from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util
from google.appengine.ext.webapp import template
from google.appengine.api import users
from google.appengine.api import memcache

from fantasm import fsm
from fantasm.action import FSMAction
from fantasm.action import DatastoreContinuationFSMAction
from mapreduce import control
from mapreduce import model
from pl import JoinPipeline

def get_class(num_entities):
  if num_entities >= 1000000:
    return "TableOne1M"
  if num_entities >= 100000:
    return "TableOne100K"
  if num_entities >= 10000:
    return "TableOne10K"
  if num_entities >= 1000:
    return "TableOne1K"
  if num_entities >= 100:
    return "TableOne100"
 
# Joins for mr, pipeline, and fsm
class Join(webapp.RequestHandler):
  def get(self):
    user = users.get_current_user()
    if not user:
      self.redirect(users.create_login_url(dest_url="/"))
      return
    q = JoinDataSet.all()
    q.order('-start')
    results = q.fetch(1000)
    datasets = [result for result in results]
    logging.info(str(datasets))
    datasets_len = len(datasets)
    q = Record.all()
    q.filter('benchmark =', "join")
    q.order('-start')
    results = q.fetch(1000) 
    records = [result for result in results]
    records_len = len(records)
    self.response.out.write(template.render("templates/join.html",
                                            {"user": user.email(),
                                             "datasets_len" : datasets_len,
                                             "datasets" : datasets,
                                             "records": records,
                                             "records_len" : records_len}))
  def post(self):
    if self.request.get("fsm_cleanup"):
      if fsm_calculate_run_time():
        self.redirect('/join')
      else:
        self.response.out.write("Error calculating fsm/wordcount")
      return

    if self.request.get("compute"):
      engine = self.request.get("engine")
      dataset = self.request.get("dataset")
      user = self.request.get('user')
      data = JoinDataSet.get_by_key_name(dataset)
      
      record = Record(engine_type=engine, 
                      dataset=dataset,
                      benchmark="join",
                      num_entities=data.num_entries,
                      #shard_count=data.num_pipelines,
                      entries_per_pipe=data.entries_per_pipe,
                      user=user,
                      state="Running")
      if engine == "fsm":
        record.put()
        context = {}
        context['user'] = str(user)
        context['num_entries'] = int(data.num_entries)
        fsm.startStateMachine('Join', [context])
        self.redirect('/join')
      elif engine == "pipeline":
       
        mypipeline = JoinPipeline(int(data.num_entries))
        mypipeline.start()
        record.pipeline_id = mypipeline.pipeline_id
        record.put()
        #self.redirect(mypipeline.base_path + "/status?root=" + mypipeline.pipeline_id)
        self.redirect('/join')
      elif engine == "mr":
        # Why 1k each per shard or less? is this ideal?
        shards = 10
        if data.num_entries > 1000:
          shards = data.num_entries/1000
        kind = get_class(data.num_entries)
        mapreduce_id = control.start_map(
          name="Join mappers",
          handler_spec="join.mr.join_mapper",
          reader_spec="mapreduce.input_readers.DatastoreInputReader",
          mapper_parameters={
              "entity_kind": "data.join."+kind,
              "processing_rate": 500
          },
          mapreduce_parameters={model.MapreduceSpec.PARAM_DONE_CALLBACK:
                     '/join/mr/callback'},
          shard_count=shards,
          queue_name="default",
        )

        record.mr_id = mapreduce_id
        record.put()
        self.redirect('/join')

        
        

class FSMMapperCallBack(webapp.RequestHandler):
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
        t = memcache.get('fsm_mapper_cleanup')
        if not t:
          logging.error("Unable to get datetime from memcache")
          return False
        dt, msec = t.split(".")
        dt = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        msec = datetime.timedelta(microseconds = int(msec))
        fullDatetime = dt + msec
        ii.end = fullDatetime

        delta = (ii.end - ii.start)
        ii.total = float(delta.days * 86400 + delta.seconds) + float(delta.microseconds)/1000000
        ii.state = "Done"
        ii.put()

        logging.info("updated: record for MR job id %s"%name)
    else:
      logging.info("Unable to find record for MR job id %s"%name)

def fsm_mapper(entity):
  max_time = memcache.get('fsm_mapper_cleanup')
  if max_time is None or max_time < str(entity.modifiedDate):
    memcache.set('fsm_mapper_cleanup', str(entity.modifiedDate)) 
    
def fsm_calculate_run_time():
  # Get the last job which was run for wordcount/fsm
  q = Record.all()
  q.filter('engine_type =','fsm')
  q.filter('benchmark =','join')
  q.order('-start')
  results = q.fetch(1)
  if len(results) == 0:
    logging.error("Unable to find a record for fsm/wordcount")
    return False

  for ii in results:
    ii.state = "Calculating time"
    ii.put()
    shards = ii.num_entities/1000
    if shards < 1:
      shards = 1
    if shards > 256:
      shards = 256 # max amount of shards allowed

    kind  = get_output_class(ii.num_entities)
    mapreduce_id = control.start_map(
            name="FSM Join cleanup",
            handler_spec="join.fsm_mapper",
            reader_spec="mapreduce.input_readers.DatastoreInputReader",
            mapper_parameters={
                "entity_kind": "data.join."+kind,
                "processing_rate": 500
            },
            mapreduce_parameters={model.MapreduceSpec.PARAM_DONE_CALLBACK:
                       '/join/fsm/callback'},
            shard_count=shards,
            queue_name="default",
          )
    ii.mr_id = mapreduce_id
    ii.put()
  return True 
