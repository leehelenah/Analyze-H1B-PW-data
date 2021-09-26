import os
from spark_utils import *
from pyspark.sql import functions as F

spark = create_spark_session()
path = 's3a://helenaudacitybucket/processed_data'

def test_zillow_num_row():
    df = spark.read.parquet(os.path.join(path, 'Zillow_price_rent'))
    assert df.count() > 0
    
def test_PW_num_row():
    df = spark.read.parquet(os.path.join(path, 'PW'))
    assert df.count() > 0
        
def test_LCA_num_row():
    df = spark.read.parquet(os.path.join(path, 'LCA'))
    assert df.count() > 0
    
def test_zillow_num_col():
    df = spark.read.parquet(os.path.join(path, 'Zillow_price_rent'))
    assert len(df.columns) > 0
    
def test_PW_num_col():
    df = spark.read.parquet(os.path.join(path, 'PW'))
    assert len(df.columns) > 0
    
def test_LCA_num_col():
    df = spark.read.parquet(os.path.join(path, 'LCA'))
    assert len(df.columns) > 0