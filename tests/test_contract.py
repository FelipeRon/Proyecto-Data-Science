import importlib.util
from pathlib import Path
spec=importlib.util.spec_from_file_location('ingest',Path(__file__).parents[1]/'kestra/scripts/ingest.py')
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)

def test_profile_exposes_missing_duplicate_and_disagreement():
    row=dict(zip(m.COLUMNS,['1','L001','L','300','310','1500','40','100','1','0','0','0','0','0']))
    other=dict(row); other['Torque [Nm]']=''
    q=m.profile([row,other])
    assert q['duplicate_udi_extra_rows']==1
    assert q['missing_by_column']['Torque [Nm]']==1
    assert q['invalid_rows']==1
    assert q['target_mode_disagreement']==2

def test_extract_rejects_schema_drift(tmp_path):
    p=tmp_path/'bad.csv'; p.write_text('UDI,wrong\n1,x\n')
    import pytest
    with pytest.raises(ValueError,match='contract changed'): m.extract(p)
