# Analyze-H1B-PW-data

## Objectives
Most of my income spend on rent. I would like to know if the same happen to all Americans and immigrants. Therefore, I load the income, rent, and price data to Spark. I believe we can come up with some ideas to be rich by looking into this data. 

## Data Source
**PW** - Data from Department of Labor. Definition: The prevailing wage is used as a measure of the minimum allowable wage to be paid by employers seeking to employ a foreign national in H-1B status. It is a calculation of the average wage rate paid by employers to similarly-employed workers in substantially comparable jobs in the geographic area of intended employment. Size: 4 files, 600k rows. <br>

**LCA** - Data from Department of Labor, disclose H1B applicants' income and demographic information. Size: 2 files, 460k rows. <br>

**Home Rent and Price** - Data from Zillow. Size: 2 files, 60k rows. <br>

## Data Quality Checks and ETL pipeline:

### step 1: Get the wage data
download the data from Department of Labor <br>
exploratory data analysis <br>

### step 2: Get the housing data
download the data from Zillow <br>
exploratory data analysis <br>

### step 3: Build an ETL pipeline for the wage data
Extract data from S3 -> process them using Spark -> load the data back into S3 <br>

Data Quality Checks for LCA: <br>
* CASE_STATUS must be certified
* must have DECISION_DATE
* must have JOB_TITLE, SOC_CODE, SOC_TITLE, FULL_TIME_POSITION
* FULL_TIME_POSITION must be true
* must have WORKSITE_COUNTY, WORKSITE_STATE, WORKSITE_POSTAL_CODE, EMPLOYER_NAME

Data Quality Checks for PW: <br>
* must have PWD_SOC_CODE, PWD_WAGE_RATE
* must have PRIMARY_WORKSITE_CITY, PRIMARY_WORKSITE_STATE, PRIMARY_WORKSITE_POSTAL_CODE
* CASE_STATUS must be Determination Issued, cannot be Withdrawn
* must have EMPLOYER_LEGAL_BUSINESS_NAME
* must have EMPLOYER_CITY, EMPLOYER_STATE, EMPLOYER_POSTAL_CODE
* EMPLOYER_COUNTRY must be UNITED STATES OF AMERICA
* must have JOB_TITLE, SUGGESTED_SOC_CODE, SUGGESTED_SOC_TITLE

### step 4: Build an ETL pipeline for the housing data
Extract data from S3 -> process them using Spark -> load the data back into S3 <br>

### step 5: Data Quality Checks - If data schema of every table matches data model


## Data Scehma
I choose star schema. It's a greate choice as I'm going to join the tables and perform analysis. It's easy to understand and build.
![image](schema.jpg)

**Fact table - Zillow_price_rent** - Data from Zillow, average home rent and price by zipcode <br>
| Column Name   | Data Type | Description                                                |
|---------------|-----------|------------------------------------------------------------|
| Zipcode       | long      | Zip Code in United States                                  |
| State         | string    | State (Abbreviation)                                       |
| Metro         | string    | Metropolitan area                                          |
| CountyName    | string    | County                                                     |
| 2021_07_Price | double    | Zillow home value index in 2020/07 (Adjusted, Smoothed)    |
| 2021_07_Rent  | double    | Zillow observed rent index in 2020/07 (Adjusted, Smoothed) |


**Dimensional table - PW** - Prevailing Wage <br>
CASE_STATUS, PWD_SOC_CODE, PWD_WAGE_RATE, PRIMARY_WORKSITE_CITY, PRIMARY_WORKSITE_COUNTY, PRIMARY_WORKSITE_STATE, PRIMARY_WORKSITE_POSTAL_CODE, EMPLOYER_CITY, EMPLOYER_STATE, EMPLOYER_POSTAL_CODE, EMPLOYER_COUNTRY, BUSINESS_NAME, JOB_TITLE, SUGGESTED_SOC_CODE, SUGGESTED_SOC_TITLE, PRIMARY_EDUCATION_LEVEL, OTHER_EDUCATION, MAJOR, SECOND_DIPLOMA, SECOND_DIPLOMA_MAJOR, FILE_SOURCE, WORKSITE_COUNTY_UPPER

**Dimensional table - LCA** - H1B applicant's data <br>
CASE_STATUS, DECISION_DATE, JOB_TITLE, SOC_CODE, SOC_TITLE, FULL_TIME_POSITION, WORKSITE_COUNTY, WORKSITE_STATE, WORKSITE_POSTAL_CODE, EMPLOYER_NAME, WAGE_RATE_OF_PAY_FROM, WAGE_RATE_OF_PAY_TO, WAGE_UNIT_OF_PAY, PREVAILING_WAGE, PW_UNIT_OF_PAY, ANNUAL_INCOME, FILE_SOURCE, WORKSITE_COUNTY_UPPER


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
 --> CAP stands for Consistency, Availability and Partition Tolerance. In general, its impossible for a distributed system to guarantee above three at a given point. Based on the CAP theory, we can choose a database that that allows for greater accessibility, such as Cassandra. <br>

## Why Spark
Spark is suitable for large dataset and supports parallel operations. Also, it supports sql queries. As I am going to further analyze the data, this is quite helpful.

## Appendix:
This project was inspired by
https://pttcareer.com/oversea_job/M.1601580566.A.B11.html
