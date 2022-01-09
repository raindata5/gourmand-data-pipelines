# Business Data Architecture

<p align="center">
<a href="#overview">Overview</a> •
  <a href="#data-architecture-diagram">Data Architecture Diagram</a> •
    <a href="#concepts">Concepts</a> •
    <a href="#prerequisites">Prerequisites</a> •
    <a href="#set-up">Set-up</a> •
  <!-- <a href="#scenario">Scenario</a> •
  <a href="#base-concepts">Base Concepts</a> •
  
  
  <!-- <a href="#installation">Installation</a> •
  <a href="#airflow-interface">Airflow Interface</a> •
  <a href="#pipeline-task-by-task">Pipeline Task by Task</a> •
  <a href="#shut-down-and-restart-airflow">Shut Down and Restart Airflow</a> •
  <a href="#learning-resources">Learning Resources</a> --> -->
</p>

---

## Overview
This projects look to leverage multiple tools to create a data architecture that could help serve as the backend for a business. While the initial focus was simply ony creating an data pipeline to move the data from one source <em>x</em> to a data warehouse <em>y</em> the project has evolved to include a number of supplementary technologies/features partially due to problems that arose out of the blue. Fortunately while there may not always be meer solutions to everything there's always a nice trade-off i.e. compromise.\
To begin we'll extract data from 3 sources ,namely, the **Census API** , a [web page]("https://www.50states.com/abbreviations.htm") via webscrape, and the **Yelp API** and push this to a **Postgres** database.\
Initially this will be used to simulate a source db using **dbt** to normalize the data as would be expected in most OLTP dbs.\
With that setup data from yelp will be pulled daily and inserted into this source db.\
From there the data will be extracted to an **S3 Bucket** which will serve as our de-facto data lake.\
Subsequently the data will be pulled from the **S3 Bucket** and ingested into some staging tables in Google Big Query ,that will serve as our data warehouse, where we'll make use of **dbt** to denormalize the data into a Snowflake Model.\
Once this is done some simple data validation checks will be carried out and we'll log these results and route them back to our **Postgres** db to create some metrics with them and send notifications via flask in the event of any issues.\
To orchestrate our recurring data workflow we'll use **Apache Airflow** with a **Postgres** Instance that is ran in a **Docker** container.\
Also using the **FastAPI** framework we'll be able to create an API on top of our source database with **Redis** to cache certain responses.\
There are also other plans to extend this project which can be seen in the following data architecture diagram.

<!-- [link](#sample)
### sample -->

---

## Data Architecture Diagram
![Alt Image text](data-pipeline-acrhitecture.drawio.png "Architecture Diagram")


## Concepts
 - [Data Pipeline]()
 - [Database Migration]()
 - [hold]()
 - [hold]()
 - [hold]()
 - [hold]()
 - [hold]()
 - [hold]()
 - [hold]()
 - [hold]()
 - [hold]()

## Prerequisites
 - [Docker]()
 - [Docker Compose]()
 - [Census API Key]()
 - [Yelp API Key]()
 - [Postgres Database w/ PGAdmin (psql may work fine)]()
 - [AWS S3 Bucket]()
 - [Google Big Query]()
 - [redis]()
 - [Python (needed libraries will be contained in requirements.txt)]()
 - [Python Virtuals Environment/s]()
 <!-- - [hold]()
 - [hold]() -->

## Set-up
At the moment this project spans several git repositories. Partially because a service-oriented architecture seemed more desirable and also as explained in the <a href="#overview">Overview</a> the project naturally evolved as I incorporated other concepts and technologies that I've learned.Nonetheless I certainly have plans to condense these repos into only in the future so both a grouped repo and the individual ones will remain available.\
For the time being here is the totality of the repositories that will be referenced\

- [Source DB Creation](https://github.com/raindata5/Gourmand-OLTP)
- [DWH Modeling](https://github.com/raindata5/gourmand-dwh)
- [API Creation](https://github.com/raindata5/gourmand-api)
- [Data Pipeline](https://github.com/raindata5/gourmand-data-pipelines)
- [Data Analysis](https://github.com/raindata5/data-analysis-business-economics-policy)
- [Data Orchestration with Apache Airflow](https://github.com/raindata5/pipeline-scripts)

1. Setting up the OLTP database \
Initially I was planning on creating this db using Microsoft SQL Server but fortunately I was able to use [Alembic]() to help with the db migration and it has the benefit of also helping set up this project.
    1. First clone the repo by doing `git clone https://github.com/raindata5/Gourmand-OLTP.git`
    2. Now go into the directory of this repo if you're not already there
    3. Here you're going to want to activate your virtual env and run `pip install -r requirements.txt`
    4. Now you can run `alembic upgrade head` and this will create the db staging tables
        > Note: Going forward when using alembic you can always delete my alembic config file and revision folder and run `alembic init` to have your own revison history
    5. To begin with ingesting the data leave your current directory and run `git clone https://github.com/raindata5/gourmand-data-pipelines`, change into this directory
    Now create a .conf file containing your yelp_credentials and census_credentials in your current directory
    6. With your python virtual environment enabled the following commands in the following order
        1. `python census-api.py`
        2. `python abreviaturas_de_estados.py`
        3. `python yelp-api-scrape.py not_daily`
    7. With the data contained in our local file system we can ingest it with the help of the following sql file [**/gourmand-data-pipelines/initial-postgres-load.sql**](https://github.com/raindata5/gourmand-data-pipelines/blob/master/initial-postgres-load.sql) which uses the [COPY]() command
    8. Head over to your user home directory and create a `profiles.yml` file
    Refer to the following [documentation]() from dbt to set it up properly
    9. Return Gourmand-OLTP and run `dbt test` just to make sure everything has gone well up until now \
    if they're no errors you can now run `dbt run`.
        >Note: Some tests are included but these may be modified, it is your option whether to run `dbt test` or not.
    10. The tables in _Production are not quite done yet as they need some modifcations such as index ,etc.\
    Now we'll open the sql file [**gourmand-data-pipelines/ddl-postgres.sql**]() and run everything here **EXCEPT** the last 2 statements under the asterisks.\
    These were added because it seemed there was an extra businessid getting added which was not adhering to the FK constraint so it was simpler to just remove it from our bridge table since it doesn't refer to any business in particular
        >OLTP dbs are not the best fit for many indexes so care was taken to use them sparingly

2. Setting up Big Query
    1. Before continuing with any more data movement we're going to have to create some tables in our DWH as we will be using a mixture of incrementals loads and also full refresh for other models\
    After having set up a project and dataset(schema) you can run these [DDL](https://console.cloud.google.com/bigquery?sq=99638138708:1c02f4f575894eccad0192b7682338cc) commands and also the following [DDL](https://console.cloud.google.com/bigquery?sq=99638138708:3ee74f2d20be43928d42de15d6dcf867) for the date dimension. You'll notice that our DWH more closely ressembles a [Snowflake Model]()\
    2.Now we'll head back to our user home directory and swap out our `profiles.yml` file using this [template]() from dbt and also following their [instructions]() to get access to our DWH in BQ

3. First Data Extraction from Postgres to Big Query
4. First Data Models in BQ
5. Setting Up Apache Airflow w/ docker
6. Setting up Redis
7. API deployment through CI/CD pipeline with Github Actions


        

     




