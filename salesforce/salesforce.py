import beatbox,time
from decimal import Decimal
import dateutil.parser

username = LOGIN_EMAIL
password = LOGIN_PASSWORD and LOGIN_TOKEN
sf = beatbox._tPartnerNS
svc = beatbox.Client()
svc.login(username, password)

def get_objects():
  dg = svc.describeGlobal()
  for t in dg:
    print(str(t))

def get_schema(table_name):
  cols = []
  ts = svc.describeSObjects(table_name)
  for f in ts[sf.fields:]:
    col = {}
    col['name'] = str(f[sf.name])
    col['type'] = str(f[sf.type])
    col['length'] = str(f[sf.length])
    col['precision'] = str(f[sf.precision])
    col['scale'] = str(f[sf.scale])
    col['python'] = get_python_type(col['type'])
    col['sql_type'] = get_sql_type(col['type'],col['length'], col['precision'], col['scale'])
    cols.append(col)
  keys = [o['name'] for o in cols]
  key = None
  if 'SystemModstamp' in keys:
    key = 'SystemModstamp'
  elif 'LastModifiedDate' in keys:
    key = 'LastModifiedDate'
  elif 'CreatedDate' in keys:
    key = 'CreatedDate'
  return cols, key

def text_type(type,length):
  if (length) <= 255:
    return 'varchar({0})'.format(length)
  elif (type) == 'textarea':
    return 'varchar'
  elif (type) == 'string':
    return 'text'

def get_sql_type(type, length, precision = None, scale = None):
  t = {
    'id' : 'char({0})'.format(length),
    'boolean' : 'boolean',
    'reference' : 'char({0})'.format(length),
    'string' : text_type(type,length),
    'textarea' : text_type(type,length),
    'picklist' : 'varchar({0})'.format(length),
    'datetime' : 'timestamptz',
    'date' : 'date',
    'int' : 'int',
    'multipicklist' : 'text',
    'currency' : 'decimal({0},{1})'.format(precision,scale),
    'percent' : 'int',
    'double' : 'float',
    'phone' : 'varchar(40)',
    'url' : 'varchar({0})'.format(length),
    'email' : 'varchar({0})'.format(length),
    'address' : 'varchar',
    'combobox' : 'varchar',
    'anytype' : 'varchar',
    'base64' : 'varchar'
  }
  return t[type.lower()]

def get_python_type(type):
  t = {
    'id' : 'string',
    'boolean' : 'bool',
    'reference' : 'string',
    'string' : 'string',
    'textarea' : 'string',
    'picklist' : 'string',
    'datetime' : 'datetime',
    'date' : 'datetime',
    'int' : 'int',
    'multipicklist' : 'string',
    'currency' : 'decimal',
    'percent' : 'int',
    'double' : 'float',
    'phone' : 'string',
    'url' : 'string',
    'email' : 'string',
    'address' : 'string',
    'combobox' : 'string',
    'anytype' : 'string',
    'base64' : 'string'
  }
  return t[type.lower()]

def generate_create_script(table, schema):
  cols = ""
  for c in schema:
    cols = cols + "\"{0}\" {1} {2},".format(c['name'],c['sql_type'],'PRIMARY KEY' if c['name'] == 'Id' else '')

  cols = cols[:-1]
  query = "CREATE TABLE \"sf_{0}\" ( {1} )".format(table, cols)
  return query

def dump_query_result(qr, t, schema, sql):
  api_calls = 0
  #print "Total number of records = " + str(qr[sf.size])
  result = []
  for rec in qr[sf.records:]:
    result.append(rec)
  query = build_insert(result, t, schema)
  sql.execute_bulk(query, no_tz=True)
  while (str(qr[sf.done]) == 'false'):
    result = []
    qr = svc.queryMore(str(qr[sf.queryLocator]))
    api_calls = api_calls + 1
    for rec in qr[sf.records:]:
      result.append(rec)
    query = build_insert(result, t, schema)
    sql.execute_bulk(query, no_tz=True)
  result = []
  return api_calls

def dump_query_result_delta(qr, t, schema, sql):
  api_calls = 0
  #print "Total number of records = " + str(qr[sf.size])
  result = []
  for rec in qr[sf.records:]:
    result.append(rec)
  query = build_update(result, t, schema)
  sql.execute_bulk(query, no_tz=True)
  while (str(qr[sf.done]) == 'false'):
    result = []
    qr = svc.queryMore(str(qr[sf.queryLocator]))
    api_calls = api_calls + 1
    for rec in qr[sf.records:]:
      result.append(rec)
    query = build_update(result, t, schema)
    sql.execute_bulk(query, no_tz=True)
  result = []
  return api_calls

def generate_select_delta(object_name, schema, start_date,end_date):
  query = generate_select(object_name, schema)
  keys = [o['name'] for o in schema]
  if 'SystemModstamp' in keys:
    query = query + " WHERE SystemModstamp > {0} AND SystemModstamp <= {1}".format(start_date.strftime('%Y-%m-%dT%H:%M:%SZ'), end_date.strftime('%Y-%m-%dT%H:%M:%SZ'))
  elif 'LastModifiedDate' in keys:
    query = query + " WHERE LastModifiedDate > {0} AND LastModifiedDate <= {1}".format(start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),  end_date.strftime('%Y-%m-%dT%H:%M:%SZ'))
  elif 'CreatedDate' in keys:
    query = query + " WHERE CreatedDate > {0} AND CreatedDate <= {1}".format(start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),  end_date.strftime('%Y-%m-%dT%H:%M:%SZ'))   
  return query

def get_changed_data(object_name, table_name, schema, sql, start_date, end_date):
  q = generate_select_delta(object_name, schema, start_date, end_date)
  query = svc.queryAll(q)
  api_calls = 1
  api_calls = api_calls + dump_query_result_delta(query, table_name, schema, sql)
  return api_calls

def generate_select(object_name, schema):
  query = "SELECT "
  for c in schema:
    query = query + c['name'] + ','
  query = query[:-1] + " FROM " + object_name
  return query

def get_data(object_name, table_name, schema, sql):
  q = generate_select(object_name,schema)
  query = svc.queryAll(q)
  api_calls = 1
  api_calls = api_calls + dump_query_result(query,table_name,schema,sql)
  return api_calls

def build_insert(data, table_name, schema):
  result = []
  cols = ""
  vals = ""
  for c in schema:
    cols = cols + "\"{0}\",".format(c['name'])
    vals = vals + "%s,"
  cols = cols[:-1]
  vals = vals[:-1]
  insert = "INSERT INTO \"sf_{0}\" ({1}) VALUES ({2})".format(table_name, cols, vals)
  for r in data:
    q = {}
    q['q'] = insert
    rec = []
    for i,v in enumerate(r[2:]):
      if v:
        if schema[i]['python'] == "string":
          rec.append(unicode(v))
        elif schema[i]['python'] == "int":
          rec.append(int(str(v).split('.')[0]))
        elif schema[i]['python'] == "float":
          rec.append(float(str(v)))
        elif schema[i]['python'] == "decimal":
          rec.append(Decimal(str(v)))
        elif schema[i]['python'] == "bool":
          if str(v) == 'false':
            rec.append(False)
          elif str(v) == 'true':
            rec.append(True)
          else:
            rec.append(None)
        elif schema[i]['python'] == "datetime":
          rec.append(dateutil.parser.parse(str(v)).strftime('%Y-%m-%d %H:%M:%S %z'))
      else:
        rec.append(None)
    q['p'] = rec
    result.append(q)
  return result
 
def build_update(data, table_name, schema):
  result = []
  cols = ""
  vals = ""
  for c in schema:
    cols = cols + "\"{0}\",".format(c['name'])
    vals = vals + "%s,"
  cols = cols[:-1]
  vals = vals[:-1]
  insert = "DELETE FROM \"sf_{0}\" WHERE \"Id\" = %s; INSERT INTO \"sf_{0}\" ({1}) VALUES ({2})".format(table_name, cols, vals)
  for r in data:
    q = {}
    q['q'] = insert
    rec = [str(r[2:][0])]
    for i,v in enumerate(r[2:]):
      if v:
        if schema[i]['python'] == "string":
          rec.append(unicode(v))
        elif schema[i]['python'] == "int":
          rec.append(int(str(v).split('.')[0]))
        elif schema[i]['python'] == "float":
          rec.append(float(str(v)))
        elif schema[i]['python'] == "decimal":
          rec.append(Decimal(str(v)))
        elif schema[i]['python'] == "bool":
          if str(v) == 'false':
            rec.append(False)
          elif str(v) == 'true':
            rec.append(True)
          else:
            rec.append(None)
        elif schema[i]['python'] == "datetime":
          rec.append(dateutil.parser.parse(str(v)).strftime('%Y-%m-%d %H:%M:%S %z'))
      else:
        rec.append(None)
    q['p'] = rec
    result.append(q)
  return result
 