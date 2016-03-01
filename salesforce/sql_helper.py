import os,sys,inspect,re,datetime
import configparser,argparse
import psycopg2
import psycopg2.extras
from collections import OrderedDict


class psql_helper:

  def __init__(self, env):
    self.host = configs.get('psql_host')
    self.user = configs.get('psql_user',None)
    self.password =  configs.get('psql_password',None)
    self.database =configs.get('psql_db',None)
   
  def execute(self, query, params=None):
    return self._execute(query=query, params=params, fetch=False)
  
  def fetch(self, query, params=None, dict=False):
    return self._execute(query=query, params=params, fetch=True, dict=dict)

  def execute_bulk(self, querys, no_tz=False):
    try:
      with psycopg2.connect(host = self.host, user = self.user, password = self.password, database = self.database) as dbconn:
        dbconn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        with dbconn.cursor() as cur:
          if no_tz:
            cur.execute("SET TIME ZONE 'PDT8PST';") 
          for q in querys:
              if "p" in q and q["p"] is not None:
                cur.execute(q["q"],tuple(q["p"]))
              else:
                cur.execute(q["q"])
    except Exception:
      raise

  def _execute(self, query, params=None, fetch=False, dict=None):
    with psycopg2.connect(host = self.host, user = self.user, password = self.password, database = self.database) as dbconn:
      dbconn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
      dict_fact = None
      if dict:
        dict_fact = psycopg2.extras.DictCursor
      with dbconn.cursor(cursor_factory=dict_fact) as cur:
        cur.execute("SET TIME ZONE 'PDT8PST';") 
        if params:
          cur.execute(query,tuple(params))
        else:
          cur.execute(query)
        if cur.description:
          self.column_names = [desc[0] for desc in cur.description]
        if fetch:
          return cur.fetchall()

  def escape_copy_string(self,s):
    s = s.replace("\\","\\\\").replace("\n","\\n").replace("\r","\\r").replace("\t","\\t")
    return s

  def generate_insert(self, doc):
    q = "INSERT INTO {0} ({1}) VALUES ({2})"
    cols = ",".join(doc["values"].keys())
    params = doc["values"].values()
    vals = "," .join(["%s"] * len(params))
    q = q.format(doc["table"], cols, vals)
    return q, params
