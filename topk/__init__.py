import logging

from data.topk import TopKDataSet
from data.topk import get_class

from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util
from google.appengine.ext.webapp import template
from google.appengine.api import users

from fantasm import fsm
from fantasm.action import FSMAction
from fantasm.action import DatastoreContinuationFSMAction

from pl import TopKPipeline
from mapreduce import control
from mapreduce import model
from records import Record

# TopK for mr, pipeline, and fsm
class TopK(webapp.RequestHandler):
  def get(self):
    user = users.get_current_user()
    if not user:
      self.redirect(users.create_login_url(dest_url="/"))
      return
    q = TopKDataSet.all()
    q.order('-start')
    results = q.fetch(1000)
    datasets = [result for result in results]
    datasets_len = len(datasets)
    q = Record.all()
    q.filter('benchmark =', "topk")
    q.order('-start')
    results = q.fetch(1000) 
    records = [result for result in results]
    records_len = len(records)
    fsm_count = get_fsm_count()
    pl_count = get_pl_count()
    mr_count = get_mr_count()
    self.response.out.write(template.render("templates/topk.html",
                                            {"user": user.email(),
                                             "datasets_len" : datasets_len,
                                             "datasets" : datasets,
                                             "fsm_count" : fsm_count,
                                             "pl_count" : pl_count,
                                             "mr_count" : mr_count,
                                             "records": records,
                                             "records_len" : records_len}))
  def post(self):
    if self.request.get("fsm_cleanup"):
      if fsm_calculate_run_time():
        self.redirect("/topk") 
      else:
        self.response.out.write("Error calculating run time of FSM/topk") 

    if self.request.get("reset_fsm_count"):
      for c in FSMSimpleCounterShard.all():
        c.delete()
      self.redirect('/topk')
      return
    if self.request.get("reset_mr_count"):
      for c in MRSimpleCounterShard.all():
        c.delete()
      self.redirect('/topk')
      return

    if self.request.get("reset_pl_count"):
      for c in PLSimpleCounterShard.all():
        c.delete()
      self.redirect('/topk')
      return

    if self.request.get("compute"):
      engine = self.request.get("engine")
      dataset = self.request.get("dataset")
      user = self.request.get('user')
      data = TopKDataSet.get_by_key_name(dataset)
      
      record = Record(engine_type=engine, 
                      dataset=dataset,
                      benchmark="topk",
                      num_entities=data.num_entries,
                      entries_per_pipe=data.entries_per_pipe,
                      user=user,
                      state="Running")
      if engine == "fsm":
        record.put()
        # reset count
        for c in FSMSimpleCounterShard.all():
          c.delete()

        context = {}
        context['user'] = str(user)
        context['num_entries'] = int(data.num_entries)
        fsm.startStateMachine('TopK', [context])
        self.redirect('/topk')
      elif engine == "pipeline":
        for c in PLSimpleCounterShard.all():
          c.delete()
        mypipeline = TopKPipeline(data.num_entries)
        mypipeline.start()
        record.pipeline_id = mypipeline.pipeline_id
        record.put()
        self.redirect('/topk') 
        #self.redirect(mypipeline.base_path + "/status?root=" + mypipeline.pipeline_id)
      elif engine == "mr":
        for c in MRSimpleCounterShard.all():
          c.delete()
        # Why 1k each per shard or less? is this ideal?
        if data.num_entries > 1000: shards = data.num_entries/1000
        else: shards = 1

        kind = get_class(data.num_entries)
        mapreduce_id = control.start_map(
          name="Wordcount with just mappers",
          handler_spec="topk.mr.topk_mapper",
          reader_spec="mapreduce.input_readers.DatastoreInputReader",
          mapper_parameters={
              "entity_kind": "data.topk."+kind,
              "processing_rate": 500
          },
          mapreduce_parameters={model.MapreduceSpec.PARAM_DONE_CALLBACK:
                     '/topk/mr/callback'},
          shard_count=shards,
          queue_name="default",
        )

        record.mr_id = mapreduce_id
        record.put()
        self.redirect('/topk')

def fsm_calculate_run_time():
  """ Fantasm does not give call backs when its done. Must figure it out
      with another job using the last modified date on output entities
  """
  # Get the last job which was run for topk /fsm
  q = Record.all()
  q.filter('engine_type =','fsm')
  q.filter('benchmark =','topk')
  q.order('-start')
  results = q.fetch(1)
  if len(results) == 0:
    logging.error("Unable to find a record for fsm/topk")
    return False

  q = None
  record = None
  for ii in results:
    if ii.state == "Done":
      logging.error("Last FSM end time has already been calculated")
    logging.info(str(ii.num_entities))
    q = FSMSimpleCounterShard.all()
    if not q:
      logging.error("No query returned for TopK results")
      return False
    record = ii

  max_date = None
  while True:
    results = q.fetch(1000)
    for ii in results:
      date = ii.modified
      if max_date == None or max_date < date:
        max_date = date
    if len(results) < 1000:
      break;
  if not max_date:
    logging.error("Unable to calculate the max date for FSM/topk")
    return False
  record.state = "Done"
  record.end = max_date
  delta = (record.end - record.start)
  record.total = float(delta.days * 86400 + delta.seconds) + float(delta.microseconds)/1000000
  record.put()
  return True
 
