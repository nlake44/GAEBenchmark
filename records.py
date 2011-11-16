from google.appengine.ext import db

class Record(db.Model):
  """ Any and all info about a run """
  start = db.DateTimeProperty(auto_now_add=True)
  end  = db.DateTimeProperty()
  engine_type = db.StringProperty()
  benchmark = db.StringProperty()
  dataset = db.StringProperty()
  num_entities = db.IntegerProperty()
  shard_count = db.IntegerProperty()
  total = db.FloatProperty()
  user = db.StringProperty() 
  entries_per_pipe = db.IntegerProperty()
  char_per_word = db.IntegerProperty()
  state = db.StringProperty()
  mr_id = db.StringProperty() 
  pipeline_id = db.StringProperty()
