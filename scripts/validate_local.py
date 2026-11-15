"""Run real CSV through dbt/DuckDB; no Snowflake claims."""
import argparse, hashlib, json, os, subprocess, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'kestra/scripts'))
from ingest import extract, profile
import duckdb
p=argparse.ArgumentParser();p.add_argument('csv');a=p.parse_args()
root=Path(__file__).resolve().parents[1]; out=root/'artifacts';out.mkdir(exist_ok=True)
blob,rows,source=extract(a.csv);sha=hashlib.sha256(blob).hexdigest()
report=dict(profile(rows),csv_sha256=sha,source=source,validation_scope='local mirror; UCI parity not yet confirmed')
(root/'docs/quality_results.json').write_text(json.dumps(report,indent=2))
con=duckdb.connect(str(out/'local.duckdb'))
con.execute('create schema if not exists RAW')
con.execute('create or replace table RAW.AI4I(FILE_SHA256 varchar,SOURCE_ROW integer,SOURCE_URL varchar,LOADED_AT timestamp,PAYLOAD json)')
con.executemany('insert into RAW.AI4I values (?,?,?,current_timestamp,?)',[(sha,i,source,json.dumps(row)) for i,row in enumerate(rows,1)])
con.close()
env=dict(os.environ,DBT_TARGET='local',LOCAL_DB_PATH=str(out/'local.duckdb'),DO_NOT_TRACK='1',DBT_SEND_ANONYMOUS_USAGE_STATS='false')
subprocess.run([str(root/'.venv/bin/dbt'),'build','--profiles-dir',str(root/'dbt')],cwd=root/'dbt',env=env,check=True)
con=duckdb.connect(str(out/'local.duckdb'))
for table in ['fct_reading','dim_product_type']:
 con.execute(f"copy (select * from GOLD.{table}) to '{out/table}.parquet' (format parquet)")
metrics=con.execute('select sum(case when baseline_alert=1 and machine_failure=1 then 1 else 0 end), sum(case when baseline_alert=1 and machine_failure=0 then 1 else 0 end), sum(case when baseline_alert=0 and machine_failure=1 then 1 else 0 end), sum(case when baseline_alert=0 and machine_failure=0 then 1 else 0 end) from GOLD.fct_reading').fetchone()
(root/'docs/baseline_results.json').write_text(json.dumps(dict(zip(['tp','fp','fn','tn'],metrics)),indent=2))
con.close()
print(json.dumps(report,indent=2))
