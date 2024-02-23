# Running analysis

## About
- Analysis of dataset of ultramarathon races spanning from 1798. to 2022. 
- Real time tracking of runners performance during marathon, that also incorporates information extracted from batch dataset
- Used datasets: 
    - batch: https://www.kaggle.com/datasets/aiaiaidavid/the-big-dataset-of-ultra-marathon-running?select=TWO_CENTURIES_OF_UM_RACES.csv
    - stream: https://data.world/jarnoma/half-marathon

## Infrastructure

![Architecture](docs/architecture.jpg)
- Raw batch data is stored on HDFS, processed with pyspark and stored inside Citus analytical database. Results are displayed using Metabase.
- Real time processing processes data that is being produced by Go producer and written to Kafka topics. Using PySpark, data from input topic is used to calculate real-time information of runners pace, average speed, total elevation gain, comparison between current and average speed (faster/slower) and prediction of runners finish time based on its earlier results on marathons of the same size and stats on current race. All calculated informations are written to dedicated topics and also stored inside Citus database and HDFS. 


## Running application

- Starting batch processing: run script `/batch/execute_batch.sh`
- Starting stream producer: run command ` go run /stream/producer_consumer/producer`
- Starting stream processing: run script `/stream/start_all.sh`
