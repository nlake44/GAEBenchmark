import random
import re 
import logging
import datetime

from records import Record
from google.appengine.ext import db
from google.appengine.ext import webapp

from data.join import TableOne1M
from data.join import TableOne100K
from data.join import TableOne10K
from data.join import TableOne1K
from data.join import TableOne100

from data.join import TableTwo1M
from data.join import TableTwo100K
from data.join import TableTwo10K
from data.join import TableTwo1K
from data.join import TableTwo100

from data.join import TableOut1M
from data.join import TableOut100K
from data.join import TableOut10K
from data.join import TableOut1K
from data.join import TableOut100


def join_mapper(entry):
  if entry.__class__.__name__ == "TableOne1M":
    entry2 = TableTwo1M.get_by_key_name(entry.value)
    entry3 = TableOut1M(key_name=entry.value, value1=entry.value, value2=entry2.value)
    entry3.put()

  if entry.__class__.__name__ == "TableOne100K":
    entry2 = TableTwo100K.get_by_key_name(entry.value)
    entry3 = TableOut100K(key_name=entry.value, value1=entry.value, value2=entry2.value)
    entry3.put()

  if entry.__class__.__name__ == "TableOne10K":
    entry2 = TableTwo10K.get_by_key_name(entry.value)
    entry3 = TableOut10K(key_name=entry.value, value1=entry.value, value2=entry2.value)
    entry3.put()
    
  if entry.__class__.__name__ == "TableOne1K":
    entry2 = TableTwo1K.get_by_key_name(entry.value)
    entry3 = TableOut1K(key_name=entry.value, value1=entry.value, value2=entry2.value)
    entry3.put()

  if entry.__class__.__name__ == "TableOne100":
    entry2 = TableTwo100.get_by_key_name(entry.value)
    entry3 = TableOut100(key_name=entry.value, value1=entry.value, value2=entry2.value)
    entry3.put()


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
