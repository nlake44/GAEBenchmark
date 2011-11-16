from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
import math
import logging
from records import Record

JOB_TYPES = ['wordcount', 'grep', 'subset', 'join', 'aggregate']
ENGINES = ['fsm', 'fsm_fan_in', 'mr', 'pipeline']
JOB_SIZE = [100, 1000, 10000, 100000, 1000000]

class Job():
  def __init__(self, num_jobs, t, a, ma, mi, stdev, jtype, engine, jsize):
    self.n = num_jobs
    self.total = t
    self.avg = a
    self.stdev = stdev
    self.maximum = ma
    self.minimum = mi
    self.jtype = jtype
    self.engine = engine
    self.jsize = jsize

def getTotal(points):
  total = 0
  for ii in points:
    total += float(ii)
  return total

def getAverage(points, total = None):
  if total == None:
    total = getTotal(points)
  if len(points) == 0:
    return 0
  return total/len(points)

def getStDev(points, average=None):
  total = 0;
  if average == None:
    average = getAverage(points)
  for ii in points:
    total += (float(ii) - average) * (float(ii) - average)
  if len(points) == 0:
    return 0
  return math.sqrt(total/len(points))

class PastJobs(webapp.RequestHandler):
  """ Show the results of jobs """
  def get(self):
    results = []
    for jt in JOB_TYPES:
      for e in ENGINES:
        for js in JOB_SIZE:
          q = Record.all()
          q.filter('benchmark =', jt)          
          q.filter('engine_type =', e)
          q.filter('num_entities =', js)
          # hope you didnt run 1k+ jobs
          ents = q.fetch(1000)
          if len(ents) == 0:
            continue
          n = 0
          sum_x = 0
          mean = 0
          stdev = 0
          maximum = 0
          minimum = 99999999
          points = []
          for ii in ents:
            if ii.total: # some jobs may have failed
              n += 1
              points.append(ii.total)
              if ii.total > maximum:
                maximum = ii.total
              if ii.total < minimum:
                minimum = ii.total
          if n != 0:
            sum_x = getTotal(points)
            mean = getAverage(points, sum_x)
            stdev = getStDev(points, mean)
            results.append(Job(n, sum_x, mean, maximum, minimum, stdev, jt, e, js))
    self.response.out.write(template.render('templates/jobs.html', {'jobs':results, 'jobs_len':len(results)}))
    return
