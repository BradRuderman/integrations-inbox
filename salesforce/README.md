1. I am extremely embarassed to commit this code, I never meant for it to be public

2. The ddls required is:

  CREATE TABLE public.etl_sf_sync (
    object_name character varying  NOT NULL,
    last_sync timestamp with time zone  NULL
  );

3. You need to fill your username/password/token on salesforce.py, line 5 and 6

4. You need to fill in your SQL information on the sql_helper.py lines 11-14

5. The depencencies are:
pytz
psycopg2
Beatbox


Sorry the code quality, architecture, and everything is just not pretty!