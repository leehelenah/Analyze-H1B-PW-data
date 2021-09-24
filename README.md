# Analyze-H1B-PW-data

## Objectives
Most of my income spend on rent. I would like to know if the same happen to all Americans and immigrants. Therefore, I load the income, rent, and price data to Spark. I believe we can come up with some ideas to be rich by looking into this data. 

## Data Source
**PW** - Data from Department of Labor. Definition: The prevailing wage is used as a measure of the minimum allowable wage to be paid by employers seeking to employ a foreign national in H-1B status. It is a calculation of the average wage rate paid by employers to similarly-employed workers in substantially comparable jobs in the geographic area of intended employment. Size: 4 files, 600k rows. <br>

**LCA** - Data from Department of Labor, disclose H1B applicants' income and demographic information. Size: 2 files, 460k rows. <br>

**Home Rent and Price** - Data from Zillow. Size: 2 files, 60k rows. <br>

## Data Quality Check and ETL pipeline:

### step 1: Get the wage data
download the data from Department of Labor
exploratory data analysis

### step 2: Get the housing data
download the data from Zillow
exploratory data analysis

### step 3: Build an ETL pipeline for the wage data
extract data from S3
process them using Spark
load the data back into S3

### step 4: Build an ETL pipeline for the housing data
extract data from S3
process them using Spark
load the data back into S3


## Data Model
**PW** - Prevailing Wage <br>
CASE_STATUS, PWD_SOC_CODE, PWD_WAGE_RATE, PRIMARY_WORKSITE_CITY, PRIMARY_WORKSITE_COUNTY, PRIMARY_WORKSITE_STATE, PRIMARY_WORKSITE_POSTAL_CODE, EMPLOYER_CITY, EMPLOYER_STATE, EMPLOYER_POSTAL_CODE, EMPLOYER_COUNTRY, BUSINESS_NAME, JOB_TITLE, SUGGESTED_SOC_CODE, SUGGESTED_SOC_TITLE, PRIMARY_EDUCATION_LEVEL, OTHER_EDUCATION, MAJOR, SECOND_DIPLOMA, SECOND_DIPLOMA_MAJOR, FILE_SOURCE, WORKSITE_COUNTY_UPPER

**LCA** - H1B applicant's data <br>
CASE_STATUS, DECISION_DATE, JOB_TITLE, SOC_CODE, SOC_TITLE, FULL_TIME_POSITION, WORKSITE_COUNTY, WORKSITE_STATE, WORKSITE_POSTAL_CODE, EMPLOYER_NAME, WAGE_RATE_OF_PAY_FROM, WAGE_RATE_OF_PAY_TO, WAGE_UNIT_OF_PAY, PREVAILING_WAGE, PW_UNIT_OF_PAY, ANNUAL_INCOME, FILE_SOURCE, WORKSITE_COUNTY_UPPER

**Zillow_price_rent** - Data from Zillow, average home rent and price by zipcode <br>
Zipcode, State, Metro, CountyName, 2021_07_Price, 2021_07_Rent

## This report shows

### Insight 1 - which company pays the best (by prevailing wage and by H1B)

### Insight 2 - which company files the most number of h1b cases

### Insight 3 - which occupation has a high prevailing wage

### Insight 4 - which zipcode area has the highest and lowest Rent-to-Price ratio

### Insight 5 - which zipcode area has the lowest Rent-to-Wage ratio (Aim to indentify the best places to live for new immigrants)

### Insight 6 - which zipcode area has the lowest Price-to-Wage ratio (Aim to indentify the best places to live for new immigrants)


## What if
The data was increased by 100x. <br>
 --> We can utilize Spark, use more cores, and process the data in parallel. <br>
The pipelines would be run on a daily basis by 7 am every day. <br>
 --> We can utilize Airflow or AWS Glue and set up daily pipeline. <br>
The database needed to be accessed by 100+ people. <br>
 --> Not a problem at all. The data is on S3. We can share access with all of them. <br>

## Why Spark
Spark is suitable for large dataset and supports parallel operations. Also, it supports sql queries. As I am going to further analyze the data, this is quite helpful.

## Appendix:
This project was inspired by
https://pttcareer.com/oversea_job/M.1601580566.A.B11.html
