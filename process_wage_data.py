import os
import query
from spark_utils import create_spark_session

def process_LCA_data(spark, input_data, output_data, view = 'LCA', mode = 'append'):
    """
    Input:
        spark: spark connection
        input_data: str, csv files
        output_data: str, parquet
    
    Output:
        a sample pandas dataframe
        
    Description:
        drop incomplete data points
        multiply by houly wage by 5,000 to derive annual income
        save the data to s3 as parquet

    """
    
    df = spark.read.format('csv').load(input_data, header='true')
    
    print('count before processing', df.count())
    
    file_source = os.path.split(input_data)[-1]

    non_na = ['CASE_STATUS','DECISION_DATE','JOB_TITLE', 'SOC_CODE', 'SOC_TITLE', 'FULL_TIME_POSITION', 
          'WAGE_RATE_OF_PAY_FROM',
          'WORKSITE_COUNTY', 'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE', 'EMPLOYER_NAME']

    df_dropna = df[['CASE_STATUS','DECISION_DATE',
                    'JOB_TITLE', 'SOC_CODE', 'SOC_TITLE', 'FULL_TIME_POSITION',
                    'WORKSITE_COUNTY', 'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE', 'EMPLOYER_NAME',
                    'WAGE_RATE_OF_PAY_FROM', 'WAGE_RATE_OF_PAY_TO','WAGE_UNIT_OF_PAY',
                    'PREVAILING_WAGE','PW_UNIT_OF_PAY']
                  ].na.drop(subset = non_na)
    
    
    df_dropna.createOrReplaceTempView(view)

    output = spark.sql(query.LCA.format(view=view, file_source=file_source))
    
    print('count after processing', output.count())
    
    output.write.parquet(os.path.join(output_data, view), mode=mode)
    
    print('write to', os.path.join(output_data, view) )
    
    return output.limit(100).toPandas()
    


def process_prevailing_wage_data(spark, input_data, output_data, view = 'PW', mode = 'append'):
    """
    Input:
        spark: spark connection
        input_data: str, csv files
        output_data: str, parquet
    
    Output:
        a sample pandas dataframe
        
    Description:
        drop incomplete data points
        save the data to s3 as parquet

    """
    
    df = spark.read.format('csv').load(input_data, header='true')
    df = df.withColumnRenamed("EMPLOYER_LEGAL_BUSINESS_NAME", "BUSINESS_NAME")
    print('count before processing', df.count())
    
    file_source = os.path.split(input_data)[-1]
    
    non_na =  ['CASE_STATUS', 'PWD_SOC_CODE', 'PWD_WAGE_RATE',
              'PRIMARY_WORKSITE_CITY', 'PRIMARY_WORKSITE_COUNTY', 'PRIMARY_WORKSITE_STATE', 'PRIMARY_WORKSITE_POSTAL_CODE',
              'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE', 'EMPLOYER_COUNTRY', 'BUSINESS_NAME',
              'JOB_TITLE', 'SUGGESTED_SOC_CODE', 'SUGGESTED_SOC_TITLE']
            
    columns = non_na + ['PRIMARY_EDUCATION_LEVEL', 'OTHER_EDUCATION', 'MAJOR', 'SECOND_DIPLOMA', 'SECOND_DIPLOMA_MAJOR']

    df_dropna = df[columns].na.drop(subset = non_na)
    
    df_dropna.createOrReplaceTempView(view)

    output = spark.sql(query.PW.format(view=view, file_source=file_source))
    
    print('count after processing', output.count())
    
    output.write.parquet(os.path.join(output_data, view), mode=mode)
    
    print('write to', os.path.join(output_data, view))
    
    return output.limit(100).toPandas()

def main():
    spark = create_spark_session()
    bucket = 's3a://helenaudacitybucket'
    
    output_LCA = os.path.join(bucket, 'processed_data')
    
    for lca_csv in ['LCA_Disclosure_Data_FY2020_Q1.csv',
                    'LCA_Disclosure_Data_FY2020_Q2.csv',
                    'LCA_Disclosure_Data_FY2020_Q3.csv',
                    'LCA_Disclosure_Data_FY2020_Q4.csv']:
        input_data = os.path.join(bucket, 'LCA', lca_csv )
        process_LCA_data(spark, input_data, output_LCA, mode='overwrite')

    
    output_PW = os.path.join(bucket, 'processed_data')
    for pw_csv in ['PW_Disclosure_Data_FY2019.csv',
                   'PW_Disclosure_Data_FY2020.csv']:
        input_data = os.path.join(bucket, 'Prevailing_Wage', pw_csv)
        process_prevailing_wage_data(spark, input_data, output_PW, mode='overwrite')


if __name__ == "__main__":
    main()
