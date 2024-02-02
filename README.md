# xlsx2bigquery
python code for importing data from xlsx 

## 사전준비
1. GCS bucket 만들기 
1. BigQuery 데이터셋 만들기

## xlsx2csv2bq.py

BigQuery에서 bq CLI 또는 SDK 에서 xlsx 파일을 직접 로드 할 수 없다. CSV 파일로 변경하고 BigQuery로 로드해야 한다. 

```bash
gcloud auth application-default login
python xlsx2csv2bq.py
```

## xlsx2pd2bq.py

xlsx에서 Dataframe 만들고 Dataframe 을 load_table_from_dataframe 해도 된다. 


```bash
gcloud auth application-default login
python xlsx2pd2bq.py
```

## CSV 파일 로드할 때 주의할 점 

 - Schema autodetection 이 잘 안되는 경우가 있습니다. 코드에서 스키마를 정확히 정의하는 것이 좋습니다. 
 - Loading CSV data using schema autodetection does not automatically detect headers if all of the columns are string types. In this case, add a numerical column to the input or declare the schema explicitly.
 - When you load CSV or JSON data, values in DATE columns must use the dash (-) separator and the date must be in the following format: YYYY-MM-DD (year-month-day).
 - When you load JSON or CSV data, values in TIMESTAMP columns must use a dash (-) or slash (/) separator for the date portion of the timestamp, and the date must be in one of the following formats: YYYY-MM-DD (year-month-day) or YYYY/MM/DD (year/month/day). The hh:mm:ss (hour-minute-second) portion of the timestamp must use a colon (:) separator.
  - Time. Columns with TIME types must be in the format HH:MM:SS[.SSSSSS].

 - Timestamp. BigQuery accepts various timestamp formats. The timestamp must include a date portion and a time portion.
The date portion can be formatted as YYYY-MM-DD or YYYY/MM/DD.
The timestamp portion must be formatted as HH:MM[:SS[.SSSSSS]] (seconds and fractions of seconds are optional).
The date and time must be separated by a space or 'T'.
Optionally, the date and time can be followed by a UTC offset or the UTC zone designator (Z). For more information, see Time zones.
For example, any of the following are valid timestamp values:

2018-08-19 12:11
2018-08-19 12:11:35
2018-08-19 12:11:35.22
2018/08/19 12:11
2018-07-05 12:54:00 UTC
2018-08-19 07:11:35.220 -05:00
2018-08-19T12:11:35.220Z

```python
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("post_abbr", "STRING"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
```

## 참조 

[Class LoadJobConfig (3.17.1)](https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.job.LoadJobConfig#google_cloud_bigquery_job_LoadJobConfig_write_disposition)