import os
import query
from spark_utils import create_spark_session
import pandas as pd

def process_zillow_data(spark, input_price, input_rent, output_join, mode='overwrite'):

    df_price = pd.read_csv(input_price)
    df_rent = pd.read_csv(input_rent)
    
    df_price = df_price[
        ['StateName', 'Metro', 'CountyName','RegionName', '2021-07-31']
    ].rename(columns={'StateName':'State','RegionName':'Zipcode', '2021-07-31':'2021_07_Price'})
    
    df_rent = df_rent[
        ['RegionName','2021-07']
    ].rename(columns={'RegionName':'Zipcode','2021-07':'2021_07_Rent'})
    
    df_join = df_price.set_index('Zipcode').join(df_rent.set_index('Zipcode'), on = 'Zipcode', how='inner').reset_index()
    
    df_join = df_join.dropna()

    
    output = spark.createDataFrame(df_join)
    
    output.write.parquet(output_join, mode=mode)
    
    return output.limit(100).toPandas()


def main():
    spark = create_spark_session()
    bucket = 's3a://helenaudacitybucket'
    input_price = os.path.join(bucket, 'Zillow', 'Zip_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv')
    input_rent = os.path.join(bucket, 'Zillow', 'Zip_ZORI_AllHomesPlusMultifamily_SSA.csv')
    output_join = os.path.join(bucket, 'processed_data','Zillow_price_rent')
    process_zillow_data(spark, input_price, input_rent, output_join, mode='overwrite')
    

if __name__ == "__main__":
    main()
