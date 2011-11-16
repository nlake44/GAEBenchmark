import re 
import pipeline
import logging
import datetime

from google.appengine.ext import db
from google.appengine.ext import webapp

from data.grep import GrepResults
from data.grep import GWords100
from data.grep import GWords1K
from data.grep import GWords10K
from data.grep import GWords100K
from data.grep import GWords1M
from data.grep import GrepDataSet
from records import Record
from mapreduce import context 

def grep_mapper(entity):
  ctx = context.get()
  needle = ctx.mapreduce_spec.mapper.params['needle']
  if needle in entity.lines:
    output = GrepResults(key_name=entity.key().name(), value=entity.lines, needle=needle)
    output.put()
 
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
