# covid-19 Case Data Analysis (Indian States)

This Spark App analyses various covid cases data and enables you to create custom mathematical insights
using a unified data structure and a trait method. After processing data it then writes to Cassandra which is then used as primary source for Data Visualization. Some analysis as part of demonstration using this app are as
follows:

- Maximum number of deaths reported among other states till Aug 29 2020.
  ![Image 4](https://imagehosting.s3.us-east-2.amazonaws.com/Maximum_death_reported.png)

- Maximum number of Recovery reported among other states till Aug 29 2020.
  ![Image 5](https://imagehosting.s3.us-east-2.amazonaws.com/maximum_recovery.png)

- Effective Increases in covid-19 Cases for all states per day
  ![Image 1](https://imagehosting.s3.us-east-2.amazonaws.com/screencapture-localhost-8080-2020-08-27-10_35_08.png)

- Minimum effective increase among other states till Aug 29 2020
  ![Image 6](https://imagehosting.s3.us-east-2.amazonaws.com/minimum_effective_increase.png)

- Effective Increases in covid-19 Cases per total tests for all states per day
  ![Image 2](https://imagehosting.s3.us-east-2.amazonaws.com/screencapture-localhost-8080-2020-08-27-10_40_27.png)
- Effective increase for state kerala
  ![Image 3](https://imagehosting.s3.us-east-2.amazonaws.com/screencapture-localhost-8080-2020-08-27-10_41_45.png)
- Effective increase per million for state of Kerala
  ![Image 4](https://imagehosting.s3.us-east-2.amazonaws.com/screencapture-localhost-8080-2020-08-27-11_40_36.png)

### Primary data source

We are currently using two APIs maintained by [covid19india](https://api.covid19india.org/)

- Confirmed cases from [states_daily_api](https://api.covid19india.org/states_daily.json)
- Recovered cases from [states_daily_api](https://api.covid19india.org/states_daily.json)
- Deceased cases from [states_daily_api](https://api.covid19india.org/states_daily.json)
- Positive cases from [state_test_data_api](https://api.covid19india.org/state_test_data.json)
- Negative cases from [state_test_data_api](https://api.covid19india.org/state_test_data.json)
- Total Tested cases from [state_test_data_api](https://api.covid19india.org/state_test_data.json)
- Total People currently in Quarantine cases from [state_test_data_api](https://api.covid19india.org/state_test_data.json)

## Installation

Inorder to run this app in local system, prequisites and correct versions are required

### Prequisites

- spark version 2.4.6 compiled with scala version 2.12
- scala 2.12
- SBT 1.3.13 or higher
- cassandra 4.0
- cqlsh 5.0.1

Download and set up Cassandra and cqlsh in your local system referring apache cassandra doc [here](https://cassandra.apache.org/doc/latest/getting_started/installing.html#prerequisites)

Start cassandra service

```sh
$ sudo service cassandra start
```

Set up cassadnra keyspace and table.

```sh
$ cqlsh
```

This would open up cassadnra cqlsh session in your terminal.
Now create a Keyspace named exactly as below (Keyspace and table names are hard coded in driver script. Any change would throw NoNodeFoundException by the datastax driver).

```sh
cqlsh> CREATE KEYSPACE covid19 WITH replication = {'class': 'SimpleStrategy', 'replication_factor':  '1'}  AND  durable_writes = true;
```

Access inside keyspace

```sh
cqlsh> USE covid19;
```

Create tables with appropriate partition key

```sh
cqlsh: covid19> CREATE TABLE state_data(property text, state_code text, state_value float, date date, PRIMARY KEY (property, state_code, date));
```

Installation is complete. you can stop cassandra service

```sh
$ sudo service cassandra stop
```

## Running locally

- git clone from master
- Rename sample-cassandra.conf inside src/main/resources folder to application.conf. Update correct values under local_cassandra object.
- start cassandra service

```sh
$ sbt compile
$ sbt package
$ sbt run local
```

Here App would start running in local machine. Fist fetching data from API, processing and finally writing to Cassandra. You can verify by logging into cqlsh and executing following

```sh
cqlsh> SELECT * FROM covid19.state_data LIMIT 100;
```

## Running on Amazon EMR Cluster with Amazon Keyspace

- Create Amazon AWS account and create an EMR instance referring this AWS Doc [here](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html)
- Set up Amazon Keyspace using this doc [here](https://docs.aws.amazon.com/keyspaces/latest/devguide/getting-started.html)
- Rename sample-cassandra.conf inside src/main/resources folder to application.conf. Update correct values under amazon_cassandra object.
- Go to project folder in your local system and build JAR file.

```sh
sbt assembly
```

- SSH into EMR master node instance and set up cassandra trustore file.

This would generate a covid19-assembly-0.1.0-SNAPSHOT.jar file in src/target folder.

- Create an Amazon S3 bucket referring to doc [here](https://docs.aws.amazon.com/AmazonS3/latest/user-guide/create-bucket.html)
- Upload covid19-assembly-0.1.0-SNAPSHOT.jar to S3 bucket.
- Start and ssh to EMR instance and download jar file from S3 bucket.

```sh
aws s3 cp your_s3_path ./
```

- execure spark submit commanf

```sh
spark-submit covid19-assembly-0.1.0-SNAPSHOT.jar aws
```

This would run the spark app and writing data to Amazon Keyspaces.
