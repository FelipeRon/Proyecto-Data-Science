import importlib.util
from pathlib import Path
import pytest
import sys
from pyspark.sql import SparkSession
spec=importlib.util.spec_from_file_location('obt',Path(__file__).parents[1]/'spark/build_obt.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
@pytest.fixture(scope='module')
def spark():
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv('PYSPARK_PYTHON',sys.executable)
        s=SparkSession.builder.master('local[2]').appName('contract-tests').getOrCreate()
        try:
            yield s
        finally:
            s.stop()
def test_dimension_duplicates_block_join(spark):
    fact=spark.createDataFrame([(1,'L')],['reading_id','product_type'])
    dim=spark.createDataFrame([('L','low'),('L','low')],['product_type','quality_tier'])
    with pytest.raises(ValueError,match='Dimension'): m.build(fact,dim)
def test_orphan_blocks_publication(spark):
    fact=spark.createDataFrame([(1,'X')],['reading_id','product_type'])
    dim=spark.createDataFrame([('L','low')],['product_type','quality_tier'])
    with pytest.raises(ValueError,match='orphan'): m.build(fact,dim)
