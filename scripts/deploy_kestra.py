"""Validate and import the flow. Supply KESTRA_USER/PASSWORD as env vars."""
import os
from pathlib import Path
import requests
root=Path(__file__).resolve().parents[1]
base=os.getenv('KESTRA_API','http://127.0.0.1:8080/api/v1/main')
s=requests.Session();s.auth=(os.environ['KESTRA_USER'],os.environ['KESTRA_PASSWORD'])
flow=(root/'kestra/flows/pipeline.yml').read_bytes()
r=s.post(base+'/flows/validate',data=flow,headers={'Content-Type':'application/x-yaml'},timeout=60)
r.raise_for_status()
result=r.json()
if not isinstance(result,list): raise ValueError('Unexpected validation response')
errors=[x for x in result if x.get('constraints') or x.get('outdated')]
if errors: raise ValueError(errors)
r=s.post(base+'/flows/import',files={'fileUpload':('pipeline.yml',flow,'application/x-yaml')},timeout=60)
r.raise_for_status()
print('Kestra flow validated and imported; full execution still needs Snowflake credentials.')
