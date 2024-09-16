# Customer Care Data Pipeline


## Problem Statement

You are a Data Engineer working with the product team in charge of our Customer Care, among others the this team ensures that pharmacy
orders are delivered on time by our couriers. If a customer needs assistance they contact an agent via chat in the app, so they can get help
with their order.
During a conversation, the agent and customer may exchange several messages. Each message is captured in an event,
Take into consideration that a conversation is unique per order. Multiple agents may be part of a conversation (ie. the conversation gets
reassigned)
The backend team provides you with access to events that their microservice emits. After discussing with the team you decide to build a POC in the form of a self-contained software project. It will be made of a
data pipeline on top of a Data Lake folder structure (for the sake of simplicity, on your local file system) to aggregate individual messages into conversations.

## Overview

I developed a self-contained data pipeline in Python to aggregate individual chat messages into conversations. This pipeline will process events and generate a summary table customer_agent_conversations with the required fields.

## The repository contains:

- Source Code
- Unit Test
- Datalake
- requirement.txt
- docker-compose.yml
- README.md

1.[Pre-requisites](#Pre_requisites)

2.[DATA_TRANSFORMATION](#DATA_TRANSFORMATION) 

4.[IMPLEMENTATION WITH PYTHON AND SQL](#IMPLEMENTATION-WITH-PYTHON-AND-SQL)

5.[STEPS-TO-RUN-CODE](#STEPS)


## Pre_requisites

- **Python 3.8+** - see [this guide](https://docs.python-guide.org/starting/install3/win/) for instructions if you're on a windows. 
- **Requirement.txt** - see [this guide](https://note.nkmk.me/en/python-pip-install-requirements/) on running a requirement.txt file.
- **Airflow** - (required for orchestration. [Airflow Installation Guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html)). Airflow was not used for this project. 
- **Docker** - (needed for contenarization). [Docker Installation Guide](https://docs.docker.com/engine/install/)).

### DATA_TRANSFORMATION

This project was aimed at generating a transformed data for the business.

The data was studied and analyzed to get an overview of the project. Findings include:

  - Some messageSentTime were not of the standard ISO format, hence a function was designed to parse the date and convert to datetime for easy transformation.
  - The priority column contains null values, a function was designed to convert the null values to false
  
  
  - There is a relationship between the customer_agent_messages and orders event data via the order_id.
    


### IMPLEMENTATION-WITH-PYTHON-AND-SQL


   This involves ingesting the data from the source to an SQL Database. This provides a memory store and processing power is shared
   by the driver node(system running the python script) and the database engine. It also provides a persistent store where other BI tools
   can easily integrate.
   
   
   
   #### STEPS
   - Include necessary database parameters in the .env file.       
   -POSTGRES_ADDRESS is the IP of the server dockers is running.  
   - Startup the [Postgres] with the docker-compose.yml file docker container. 
    
          docker-compose up
    
   - Execute the [Main.py]        
        - Create [DB Connection] and the tables
        - Processed the Dataset
        - Execute [Result.sql] script to generate the desired output
        - we are going to load the returned dataframe to a local drive and a postgreSQL DB. [data/transformed/customer_agent_conversations.csv]
 

### IMPLEMENTATION-WITH-PYSPARK
  - This method is highly efficient when integrated with HDFS. This should be considered when the dataset is very large. It supports Multiprocessing,
    hence, increasing the speed of transformation

### Pipeline Orchestration using Airflow
#### STEPS

## Setting up Airflow

### Docker Integration 
- Dockerize the pipeline components for easier deployment and management.
- Use Docker Compose to orchestrate the Docker containers for the pipeline.
. 
## Defining DAGs

- Define each pipeline workflow as a separate DAG in Airflow.
- Specify tasks and dependencies using Airflow operators and Python scripts.


## Scheduling and Automations

- Schedule the pipeline to run at specific intervals using Airflow's scheduling feature.
- Automate data processing and loading based on triggers or conditions.

## Execution Instructions

1. Trigger pipeline execution manually:
- Access the Airflow web UI at http://localhost:8080.
- Trigger the desired DAG to start the pipeline execution.

2. Set up automatic scheduling:

- Define the desired schedule for each DAG using Airflow's DAG scheduling options.




