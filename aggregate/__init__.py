import logging

from data.aggregate import AggDataSet
from data.aggregate import FSMSimpleCounterShard
from data.aggregate import MRSimpleCounterShard
from data.aggregate import PLSimpleCounterShard
from data.aggregate import get_mr_count
from data.aggregate import get_pl_count
from data.aggregate import get_fsm_count
from data.aggregate import get_class


from google.appengine.ext import db
from google.appengine.ext import webapp
from google.appengine.ext.webapp import util
from google.appengine.ext.webapp import template
from google.appengine.api import users

from fantasm import fsm
from fantasm.action import FSMAction
from fantasm.action import DatastoreContinuationFSMAction

from pl import AggregatePipeline
from mapreduce import control
from mapreduce import model
from records import Record

# Aggregate for mr, pipeline, and fsm
class Aggregate(webapp.RequestHandler):
  def get(self):
    user = users.get_current_user()
    if not user:
      self.redirect(users.create_login_url(dest_url="/"))
      return
    q = AggDataSet.all()
    q.order('-start')
    results = q.fetch(1000)
    datasets = [result for result in results]
    datasets_len = len(datasets)
    q = Record.all()
    q.filter('benchmark =', "aggregate")
    q.order('-start')
    results = q.fetch(1000) 
    records = [result for result in results]
    records_len = len(records)
    fsm_count = get_fsm_count()
    pl_count = get_pl_count()
    mr_count = get_mr_count()
    self.response.out.write(template.render("templates/aggregate.html",
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
        self.redirect("/agg") 
      else:
        self.response.out.write("Error calculating run time of FSM/aggregate") 

    if self.request.get("reset_fsm_count"):
      for c in FSMSimpleCounterShard.all():
        c.delete()
      self.redirect('/agg')
      return
    if self.request.get("reset_mr_count"):
      for c in MRSimpleCounterShard.all():
        c.delete()
      self.redirect('/agg')
      return

    if self.request.get("reset_pl_count"):
      for c in PLSimpleCounterShard.all():
        c.delete()
      self.redirect('/agg')
      return

    if self.request.get("compute"):
      engine = self.request.get("engine")
      dataset = self.request.get("dataset")
      user = self.request.get('user')
      data = AggDataSet.get_by_key_name(dataset)
      
      record = Record(engine_type=engine, 
                      dataset=dataset,
                      benchmark="aggregate",
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
        fsm.startStateMachine('Aggregate', [context])
        self.redirect('/agg')
      elif engine == "fsm_fan_in":
        """ This one uses fan in """
        record.put()
        # reset count
        for c in FSMSimpleCounterShard.all():
          c.delete()

        context = {}
        context['user'] = str(user)
        context['num_entries'] = int(data.num_entries)
        fsm.startStateMachine('Aggregate2', [context])
        self.redirect('/agg')
      elif engine == "pipeline":
        for c in PLSimpleCounterShard.all():
          c.delete()
        mypipeline = AggregatePipeline(data.num_entries)
        mypipeline.start()
        record.pipeline_id = mypipeline.pipeline_id
        record.put()
        self.redirect('/agg') 
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
          handler_spec="aggregate.mr.aggregate_mapper",
          reader_spec="mapreduce.input_readers.DatastoreInputReader",
          mapper_parameters={
              "entity_kind": "data.aggregate."+kind,
              "processing_rate": 500
          },
          mapreduce_parameters={model.MapreduceSpec.PARAM_DONE_CALLBACK:
                     '/agg/mr/callback'},
          shard_count=shards,
          queue_name="default",
        )

        record.mr_id = mapreduce_id
        record.put()
        self.redirect('/agg')

def fsm_calculate_run_time():
  """ Fantasm does not give call backs when its done. Must figure it out
      with another job using the last modified date on output entities
  """
  # Get the last job which was run for aggregate /fsm
  q = Record.all()
  q.filter('engine_type =','fsm')
  q.filter('benchmark =','aggregate')
  q.order('-start')
  results = q.fetch(1)

  # There is a second type of fsm job that has a fan in state
  q2 = Record.all()
  q2.filter('engine_type =','fsm_fan_in')
  q2.filter('benchmark =','aggregate')
  q2.order('-start')
  results2 = q2.fetch(1)
   
  if len(results) == 0 and len(results2) == 0:
    logging.error("Unable to find a record for fsm/aggregate")
    return False

  # Take only the one which ran last
  if len(results) == 0:
    results = results2 #fsm with fan in ran last
  elif len(results2) == 0:
    pass
  elif results[0].start > results2[0].start:
    pass
  else:
    results = results2 #fsm with fan in ran last

  q = None
  record = None
  # There should only be one result
  for ii in results:
    if ii.state == "Done":
      logging.error("Last FSM end time has already been calculated")
    logging.info(str(ii.num_entities))
    q = FSMSimpleCounterShard.all()
    if not q:
      logging.error("No query returned for Aggregate results")
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
    logging.error("Unable to calculate the max date for FSM/aggregate")
    return False
  record.state = "Done"
  record.end = max_date
  delta = (record.end - record.start)
  record.total = float(delta.days * 86400 + delta.seconds) + float(delta.microseconds)/1000000
  record.put()
  return True
 
