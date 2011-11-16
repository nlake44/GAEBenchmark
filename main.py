""" This program generates data into the datastore, and has different processing benchmarks on that set of data"""

__author__ = """Navraj Chohan <nlake44@gmail.com>"""

import datetime
import logging
import re
import urllib
import hashlib 

import random 

import data.wordcount
import data.aggregate
import data.grep
import data.join
import data.subset 

import wordcount.mr 
import join.mr 
import grep.mr 
import aggregate.mr 
import subset.mr 

import aggregate 
import wordcount
import grep
import join 
import subset 

import jobs 

from google.appengine.ext import blobstore
from google.appengine.ext import db
from google.appengine.ext import webapp

from google.appengine.ext.webapp import util
from google.appengine.ext.webapp import template

from google.appengine.api import taskqueue
from google.appengine.api import users

class IndexHandler(webapp.RequestHandler):
  """The main page redirects to pipeline
  """
  def get(self):
    self.response.out.write(template.render("templates/index.html",{}))
   
    return

APP = webapp.WSGIApplication(
    [
        ('/', IndexHandler),

        ('/data/wc', data.wordcount.GenerateData),
        ('/data/agg', data.aggregate.GenerateData),
        ('/data/join', data.join.GenerateData),
        ('/data/grep', data.grep.GenerateData),
        ('/data/subset', data.subset.GenerateData),

        ('/data/agg/delete_callback*', data.aggregate.DeleteDoneCallBack),
        ('/data/subset/delete_callback*', data.subset.DeleteDoneCallBack),
        ('/data/wc/delete_callback*', data.wordcount.DeleteDoneCallBack),
        ('/data/grep/delete_callback*', data.grep.DeleteDoneCallBack),
        ('/data/join/delete_callback*', data.join.DeleteDoneCallBack),

        ('/wc', wordcount.WordCount),
        ('/agg', aggregate.Aggregate),
        ('/subset', subset.SubSet),
        ('/grep', grep.Grep),
        ('/join', join.Join),
       
        ('/wc/mr/callback', wordcount.mr.MRCallback),
        ('/agg/mr/callback', aggregate.mr.MRCallback),
        ('/subset/mr/callback', subset.mr.MRCallback),
        ('/join/mr/callback', join.mr.MRCallback),
        ('/join/fsm/callback', join.FSMMapperCallBack),
        ('/grep/mr/callback', grep.mr.MRCallback),
        ('/jobs', jobs.PastJobs),
    ],
    debug=True)

def main():
  util.run_wsgi_app(APP)

if __name__ == '__main__':
  main()
