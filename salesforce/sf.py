import os,sys,inspect,datetime,json,re
from sql_helper import psql_helper
import salesforce as sf
import datetime,pytz

class Salesforce:
  sql = psql_helper()

  salesforce_objects = ['Account','Contact','Event','EventRelation','Lead','LeadHistory','Opportunity','OpportunityContactRole','OpportunityFieldHistory','OpportunityHistory','Task','TaskRelation','User','EmailServicesAddress']
  api_calls = 0

  def inital_import_all(self):
    for t in self.salesforce_objects:
      table_name = Salesforce.to_underscore(t)
      self.initial_import(t, table_name)

  def sync_latest_update(self, object_name, table_name, key, insert=False):
    if insert:
      q = """
        INSERT INTO etl_sf_sync (object_name, last_sync)
        SELECT '{0}', max("{2}")
        FROM "sf_{1}"
      """.format(object_name, table_name, key)
    else:
      q = """
        UPDATE etl_sf_sync
        SET last_sync= (SELECT max("{2}") FROM "sf_{1}")
        WHERE object_name = '{0}'
      """.format(object_name, table_name, key)
    self.sql.execute(q)

  def initial_import(self, object_name, table_name):
    print("-------------------------")
    print("Starting import for " + object_name)
    self.sql.execute("DELETE FROM etl_sf_sync WHERE object_name = %s", [object_name])
    now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    schema, dt_col = sf.get_schema(object_name)
    self.api_calls = self.api_calls + 1
    droptable = "DROP TABLE IF EXISTS \"sf_{0}\" CASCADE;".format(table_name)
    self.sql.execute(droptable)
    create = sf.generate_create_script(table_name, schema)
    self.sql.execute(create)
    self.api_calls = self.api_calls + sf.get_data(object_name, table_name, schema, self.sql)
    self.sync_latest_update(object_name, table_name, dt_col, True)

  def delta_changes(self):
    query = "SELECT * FROM etl_sf_sync"
    times = self.sql.fetch(query, None, True)
    sync_times = {}
    for t in times:
      sync_times[t[0]] = t[1]
    for object_name in self.salesforce_objects:
      table_name = Salesforce.to_underscore(object_name)
      schema, dt_col = sf.get_schema(object_name)
      self.api_calls = self.api_calls + 1
      upper_dt = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
      if object_name in sync_times:
        lower_dt = sync_times[object_name].astimezone(pytz.utc)
        self.api_calls = self.api_calls + sf.get_changed_data(object_name, table_name, schema, self.sql, lower_dt, upper_dt)
        self.sync_latest_update(object_name, table_name, dt_col)
      else:
        self.initial_import(object_name, table_name)

  @staticmethod
  def to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
  
  @staticmethod
  def to_camel_case(snake_str):
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return "".join(x.title() for x in components)

if __name__ == '__main__':
  s = Salesforce()
  #sf.get_objects()
  s.delta_changes()
  #print(s.api_calls)