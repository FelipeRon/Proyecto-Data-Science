"""Immutable snapshot extraction and transactional, idempotent RAW loading."""
import csv, hashlib, io, json, os, zipfile
from pathlib import Path
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL = 'https://archive.ics.uci.edu/static/public/601/ai4i+2020+predictive+maintenance+dataset.zip'
COLUMNS = ['UDI','Product ID','Type','Air temperature [K]','Process temperature [K]',
           'Rotational speed [rpm]','Torque [Nm]','Tool wear [min]',
           'Machine failure','TWF','HDF','PWF','OSF','RNF']

def extract(local=None):
    if local:
        blob = Path(local).read_bytes()
        source = str(local)
    else:
        session = requests.Session()
        session.mount('https://', HTTPAdapter(max_retries=Retry(
            total=3, backoff_factor=2, status_forcelist=[429,500,502,503,504])))
        response = session.get(URL, timeout=(15,120))
        response.raise_for_status()
        blob, source = response.content, URL
    if blob[:2] == b'PK':
        with zipfile.ZipFile(io.BytesIO(blob)) as z:
            candidates = [n for n in z.namelist() if n.endswith('ai4i2020.csv')]
            if len(candidates) != 1:
                raise ValueError('Expected one ai4i2020.csv')
            blob = z.read(candidates[0])
    reader = csv.DictReader(io.StringIO(blob.decode('utf-8-sig')))
    if reader.fieldnames != COLUMNS:
        raise ValueError(f'Source contract changed: {reader.fieldnames}')
    rows = list(reader)
    if not rows:
        raise ValueError('Empty source')
    return blob, rows, source

def profile(rows):
    from collections import Counter
    def missing(v): return v is None or not str(v).strip()
    counts = Counter(x['UDI'] for x in rows)
    numeric = COLUMNS[3:]
    invalid = 0
    for row in rows:
        try:
            values = [float(row[c]) for c in numeric]
            good = (row['Type'] in ['L','M','H'] and float(row['UDI']) > 0
                and all(__import__('math').isfinite(v) for v in values)
                and all(float(row[c]) > 0 for c in COLUMNS[3:7])
                and float(row['Tool wear [min]']) >= 0
                and all(row[c] in ['0','1'] for c in COLUMNS[8:]))
            invalid += not good
        except (ValueError, TypeError): invalid += 1
    return {'rows':len(rows), 'missing_by_column':{c:sum(missing(x[c]) for x in rows) for c in COLUMNS},
        'duplicate_udi_extra_rows':sum(n-1 for n in counts.values()),
        'invalid_rows':invalid,
        'target_failures':sum(x['Machine failure']=='1' for x in rows),
        'target_mode_disagreement':sum(int(x['Machine failure']) != int(any(x[c]=='1' for c in COLUMNS[9:])) for x in rows if all(x[c] in ['0','1'] for c in COLUMNS[8:])),
        'uncomparable_labels':sum(not all(x[c] in ['0','1'] for c in COLUMNS[8:]) for x in rows)}

def main():
    blob, rows, source = extract(os.getenv('SOURCE_CSV'))
    sha = hashlib.sha256(blob).hexdigest()
    out = Path(os.getenv('ARTIFACT_DIR','/app/artifacts')); out.mkdir(parents=True,exist_ok=True)
    report = dict(profile(rows), csv_sha256=sha, source=source)
    (out/'quality.json').write_text(json.dumps(report, indent=2))
    if os.getenv('PROFILE_ONLY') == '1': return
    from common import connect
    with connect() as conn:
        cur=conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS RAW.AI4I (
            FILE_SHA256 VARCHAR, SOURCE_ROW INTEGER, SOURCE_URL VARCHAR,
            LOADED_AT TIMESTAMP_TZ, PAYLOAD VARIANT)""")
        # Staging DDL precedes transaction; failed inserts cannot erase previous data.
        cur.execute('CREATE TEMP TABLE STAGE_AI4I LIKE RAW.AI4I')
        timestamp=datetime.now(timezone.utc).isoformat()
        cur.executemany('INSERT INTO STAGE_AI4I SELECT %s,%s,%s,TO_TIMESTAMP_TZ(%s),PARSE_JSON(%s)',
            [(sha,i,source,timestamp,json.dumps(row)) for i,row in enumerate(rows,1)])
        cur.execute('SELECT COUNT(*) FROM STAGE_AI4I')
        if cur.fetchone()[0] != len(rows): raise ValueError('RAW staging count mismatch')
        cur.execute('BEGIN')
        try:
            cur.execute('DELETE FROM RAW.AI4I WHERE FILE_SHA256=%s',(sha,))
            cur.execute('INSERT INTO RAW.AI4I SELECT * FROM STAGE_AI4I')
            cur.execute('COMMIT')
        except Exception:
            cur.execute('ROLLBACK'); raise
    print(json.dumps({'loaded':len(rows),'sha256':sha}))

if __name__ == '__main__': main()
