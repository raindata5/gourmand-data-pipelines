# Business Data Architecture

<p align="center">
<a href="#overview">Overview</a> •
  <a href="#data-architecture-diagram">Data Architecture Diagram</a> •
  <a href="#data-visualization">Data Visualization</a> •
    <a href="#main-concepts">Main Concepts</a> •
    <a href="#prerequisites">Prerequisites</a> •
    <a href="#set-up">Set-up</a> •
    <a href="#where-to-go-from-here">Where to go from here</a>•
    <a href="#contact">Contact</a> •
</p>

---

## Overview
This project looks to leverage multiple tools to create a data architecture that could help serve as the backend for a business and even the frontend (more on this in due time). While the initial focus was simply ony creating an data pipeline to move the data from one source <em>x</em> to a data warehouse <em>y</em> the project has evolved to include a number of supplementary technologies/features partially due to problems that arose out of the blue. Fortunately while there may not always be meer solutions to everything there's always a nice trade-off i.e. compromise.\
To begin we'll extract data from 3 sources ,namely, the **Census API** , a [web page]("https://www.50states.com/abbreviations.htm") via webscrape, and the **Yelp API** and push this to a **Postgres** database.\
Initially this will be used to simulate a source db using **dbt** to normalize the data as would be expected in most OLTP dbs.\
With that setup data from yelp will be pulled daily and inserted into this source db.\
From there the data will be extracted to an **S3 Bucket** which will serve as our de-facto data lake.\
Subsequently the data will be pulled from the **S3 Bucket** and ingested into some staging tables in Google Big Query ,that will serve as our data warehouse, where we'll make use of **dbt** to denormalize the data into a Snowflake Model.\
Once this is done some simple data validation checks will be carried out and we'll log these results and route them back to our **Postgres** db to create some metrics with them and send notifications via flask in the event of any issues.\
To orchestrate our recurring data workflow we'll use **Apache Airflow** with a **Postgres** Instance that is ran in a **Docker** container.
> Note: Now the code has been updated to run Apache Airflow completely in Docker with the CeleryExecutor 😀. So now this provides more scalibility if we plan on working with a multi-node setup. I just have yet to update the directions in this code.

Also using the **FastAPI** framework we'll be able to create an API on top of our source database with **Redis** to cache certain responses.
> Note: The our API has been fully dockerized and integrated as such with the CI/CD pipeline.

There are also other plans to extend this project which can be seen in the following data architecture diagram.

<!-- [link](#sample)
### sample -->

---

## Data Architecture Diagram
![A diagram of the data architecture created](images/data-pipeline-architecture.drawio.png "Architecture Diagram")


## Data Visualization

![A heatmap of the included businesses](images/heatmap_of_locations.png "Heatmap")
Above you can see the businesses that we're tracking which will be crucial to take into account upon evaluating external validity during any analysis.

![A dashboard showing the effects of payment level and business categories on merics](images/payment_level_differences.png "Business segments") 
A simple dashboard showing the benefits of creating a business that caters to providing low-cost food.
> Note: There are more than just restaurants included
1. Low-cost businesses generate the most amount of reviews (this can lightly be taken for customer traffic)
2. The fact that a business is a low-cost has little to no impact on it's business rating \
The majority of our businesses are in the U.S. and low-cost. This puts in perspective a statement I heard a french guy say "On dit qu'ici (c'est-a-dire la france) on vit pour manger mais eux (c'est-a-dire les Amèricains) ils mangent pour vivre" 😅.


## Main Concepts
 - [Data Pipeline](https://www.snowflake.com/guides/data-pipeline#:~:text=A%20data%20pipeline%20is%20a%20means%20of%20moving,steps%20involved%20in%20aggregating%2C%20organizing%2C%20and%20moving%20data.)
 - [Database Migration](https://www.simform.com/blog/database-migration/)
 - [Data Modeling](https://en.wikipedia.org/wiki/Data_modeling)
 - [Data Validation](https://en.wikipedia.org/wiki/Data_validation_and_reconciliation#:~:text=Data%20validation%201%20Data%20filtering.%20Data%20filtering%20denotes,Result%20validation.%20...%203%20Gross%20error%20detection.%20)
 - [Application Programming Interface (API)](https://en.wikipedia.org/wiki/API)
 - [Workflow Orchestrations](https://www.openprisetech.com/blog/what-is-data-orchestration/#:~:text=Data%20Orchestration%20is%20the%20automation%20of%20data-driven%20processes,many%20different%20systems%2C%20departments%2C%20and%20types%20of%20data.)
 - [Cache](https://en.wikipedia.org/wiki/Cache_(computing))
 - [Key/Value Store](https://en.wikipedia.org/wiki/Key%E2%80%93value_database)
 - [Logging](https://en.wikipedia.org/wiki/Key%E2%80%93value_database)
 - [Containers](https://www.docker.com/resources/what-container)
 - [Data Lake](https://aws.amazon.com/big-data/datalakes-and-analytics/what-is-a-data-lake/)
 - [Data Analysis](https://en.wikipedia.org/wiki/Data_analysis)

## Prerequisites
 - [Docker](https://docs.docker.com/engine/install/)
 - [Docker Compose](https://docs.docker.com/compose/)
 - [Census API Key](https://www.census.gov/data/developers/guidance/api-user-guide.Core_Concepts.html)
 - [Yelp API Key](https://www.yelp.com/developers/documentation/v3)
 - [Postgres Database w/ PGAdmin (psql may work fine)](https://www.postgresql.org/download/)
 - [AWS S3 Bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-bucket.html)
 - [Google Big Query](https://cloud.google.com/resource-manager/docs/creating-managing-projects#:~:text=Creating%20a%20Project%20in%20Google%20Cloud%201%20Go,entering%20new%20project%20details%2C%20click%20Create.%20See%20More.)
 - [Redis](https://redis.io/download)
 - [Python (needed libraries will be contained in requirements.txt)](https://www.python.org/downloads/)
 - [Python Virtuals Environment/s](https://docs.python.org/3/library/venv.html)
 <!-- - [hold]()
 - [hold]() -->

## Set-up
At the moment this project spans several git repositories. Partially because a service-oriented architecture seemed more desirable and also as explained in the <a href="#overview">Overview</a> the project naturally evolved as I incorporated other concepts and technologies that I've learned.Nonetheless I certainly have plans to condense these repos into only in the future so both a grouped repo and the individual ones will remain available.\
For the time being here is the totality of the repositories that will be referenced

- [Source DB Creation](https://github.com/raindata5/Gourmand-OLTP)
- [DWH Modeling](https://github.com/raindata5/gourmand-dwh)
- [API Creation](https://github.com/raindata5/gourmand-api)
- [Data Pipeline](https://github.com/raindata5/gourmand-data-pipelines)
- [Data Analysis](https://github.com/raindata5/data-analysis-business-economics-policy)
- [Data Orchestration with Apache Airflow](https://github.com/raindata5/pipeline-scripts)

1. Setting up the OLTP database \
Initially I was planning on creating this db using Microsoft SQL Server but fortunately I was able to use [Alembic]() to help with the db migration and it has the benefit of also helping set up this project. The goal is to end up with the following normalized data model shown with a very well designed ERD editor which you can find [here](https://github.com/vuerd/vuerd)

    ![Normalized data model for oltp db](images/oltp_db_model.png "Normalized Model")
    > Note: The table EventCategory isn't connected to event. We'll use this scenario in the future to show how to solve schema evolution problems
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
    10. The tables in _Production are not quite done yet as they need some modifcations such as indexes ,etc.\
    Now we'll open the sql file [**gourmand-data-pipelines/ddl-postgres.sql**]() and run everything here **EXCEPT** the last 2 statements under the asterisks.\
    These were added because it seemed there was an extra businessid getting added which was not adhering to the FK constraint so it was simpler to just remove it from our bridge table since it doesn't refer to any business in particular
        >OLTP dbs are not the best fit for many indexes so care was taken to use them sparingly

2. Setting up Big Query
    1. Before continuing with any more data movement we're going to have to create some tables in our DWH as we will be using a mixture of incrementals loads and also full refresh for other models\
    After having set up a project and dataset(schema) you can run these [DDL](https://console.cloud.google.com/bigquery?sq=99638138708:1c02f4f575894eccad0192b7682338cc) commands and also the following [DDL](https://console.cloud.google.com/bigquery?sq=99638138708:3ee74f2d20be43928d42de15d6dcf867) for the date dimension. You'll notice that our DWH more closely ressembles a [Snowflake Model]()\
    2. Now we'll head back to our user home directory and swap out our `profiles.yml` file using this [template]() from dbt and also following their [instructions]() to get access to our DWH in BQ

            {
            "type": "service_account",
            "project_id": "gourmanddwh",
            "private_key_id": "**",
            "private_key": "****",
            .....
            }

        It would be best to use the service account json method of authentication as this makes our python scripts easier to implement when authenticating with Big Query

3. First Data Extraction from Postgres to S3 to Big Query
    1. Before extracting the data from Postgres you'll want to make sure also that you've set up your S3 bucket with the proper credentials

            [aws_boto_credentials]
            access_key = '****'
            secret_key = '****'
            bucket_name = '****'
            account_id = '****'
    2. Now go in to the gourmand-data-pipelines directory and run the following scripts in order\
    `python postgres-to-s3.py`\
    `python s3-to-bigquery.py`

4. First Data Models in BQ\
Here we are going to want out data model to look like the following 

    ![](images/olap_db_model.png "Normalized Model")

    >Note: This is a regular denormalized data model and considering the options we have when it comes to storage we can always create views of more wide tables on top of these tables to facilitate analytical querying.
    
    1. The data in BQ we can now run \
    `git clone https://github.com/raindata5/gourmand-dwh.git`
    2. Switch into this directory now and having deactivated your previous virtual environment, activate one for this directory and run\
    `pip install -r requirements.txt`
    3. Run `dbt debug` to make sure everything is working well connection-wise then run `dbt test` to maje sure your source tables are fine.\
    From here you can run the following \
    `dbt snapshot`\
    `dbt run`\
    `dbt test --store-failures > dbt-test-file.txt`\
    Now you should have a fully denormalized dwh 😀
5. Setting Up Apache Airflow w/ docker
    1. To begin we'll run `git clone https://github.com/raindata5/pipeline-scripts.git `
    2. Having already set up docker and docker-compose and being going into this directory you can now run `docker-compose up -d` to start the database that Airflow will use
    3. Now you change into this directory, create a new virtual environment and run `pip install -r requirements.txt`, then create a new directory for all your apache airflow files and change into this directory.
    4. To set up Apache Airflow we're going to first follow their [instructions]() here . After you have it functioning we are going to follow these [instructions]() next to change our executor and the database we use for Airflow as well.

            executor = LocalExecutor
            sql_alchemy_conn = postgresql+psycopg2://user:pass@host:port/db

        The database connection info will mirror that of the information we specified in our docker-compose file
    5. Run the following commands to spin up Airflow \
    `airflow webserver -D --port 8080`\
    `airflow scheduler -D`\ now you'll just have to wait a bit for them both to spin up
6.  Setting up the Airflow Workflow
    1. After having initialized Apache Airflow it should have created a dags folder in your airflow directory you created previously. Now you'll just want to use both [pipeline-scripts/apache-airflow/dags/ssh_dag.py](https://github.com/raindata5/pipeline-scripts/blob/master/apache-airflow/dags/ssh_dag.py) and [pipeline-scripts/apache-airflow/dags/aa_pipeline_perf_dag.py](https://github.com/raindata5/pipeline-scripts/blob/master/apache-airflow/dags/aa_pipeline_perf_dag.py) as a template\
    Since the absolute paths will be a bit different to yours you'll want to make som adjustments there.Also specifically for *pipeline-scripts/apache-airflow/dags/ssh_dag.py* since this is connecting to an Ubuntu VM on [DigitalOcean]() , if you decide to run this locally then you'll have to switch to the bash operator.\
    These have also been set up with a web hook using slack so that's a feature that can be omitted as well.
    > Note: The python files [postgres_bq_data_validation_task]() and [bq_dbt_validation_task]() also use the slack webhook so to avoid having to make too many changes here then I reccommend just placing a random url in since in these files the slack function used has error handling so it shouldn't break anything in the pipeline. But this is only if you prefer not to use the slack webhook which you could easily set up [here]()
    2. After having following the previous instructions then you should now be able to run your dag
    > Tip: To test out your pipeline the python file [gourmand-data-pipelines/yelp-api-scrape-daily.py](https://github.com/raindata5/gourmand-data-pipelines/blob/master/yelp-api-scrape-daily.py) provides the option to pass in the argument "test" which will only carry out 300 pulls from the yelp api. In my case I would just edit the task in the dag in the following way

        yelp_business_api_pull_task = SSHOperator(
        ssh_hook=sshhook,
        task_id='yelp_business_api_pull',
        command=f"cd /home/ubuntucontributor/gourmand-data-pipelines/; {gdp_ve}/python {gdp_dir}/yelp-api-scrape-daily.py test;",
        dag=dag)
6. Setting up Redis
    1. The data pipeline being now managed by Apache Airflow, we will continue and set up Redis on our machine as a way of caching for our API.
    One just needs to simply follow these installaton [instructions]() then just run (if on WSL or Ubuntu respectively) \
    `sudo service redis-server restart` or `systemctl restart redis-server.service`
7. API deployment through CI/CD pipeline with Github Actions
    1. With redis working you can now clone the repo for our api by running 
    `git clone https://github.com/raindata5/gourmand-api.git` , changeing into this directory, creating a new virtual environment and then running `pip install -r requirements.txt`.
    2. Then you will need to create a `.env` file which will look something like the following using information for our Postgres db

            pg_oltp_api_user=***
            pg_oltp_api_password=***
            pg_oltp_api_host=***
            pg_oltp_api_port=***
            pg_oltp_api_db=***
            SECRET_KEY=dsfssdfsfs2342342234dsfdasdssfdssWqBdMXA3YJXfZTljfdsffs
            ALGORITHM=***
            ACCESS_TOKEN_EXPIRE_MINUTES=120
            pg_oltp_api_db_test=testdb

    3. Now we can start up the api with a command such as \
    `uvicorn gourmandapiapp.main:app --reload`
        > Note: Only use the *--reload* parameter in a test environment
    4. Now if you want to enable running tests on your API using pytest then we can leverage alembic to create a new test database.\
    If you keep the username, password, host, and port the same for this new test database then all you have to do is run `alembic upgrade head -c ./alembic2.ini` and this will create a copy of your previous db made in the Postgres previously. Which is pretty awesome to say the least 😀. But it gets even better and cooler. From this directory we run tests using a command like `pytest -vsx --disable-warnings gourmandapiapptests/*` which will run all the tests in this folder and allow us to make sure our API works as expected when making changes.
        > Note: When testing the API the endpoints with update/create/delete functionality actually will be persisted in the test database. Fortunately we have 2 workarounds for this.

        One: In our conftest file [gourmand-api/gourmandapiapptests/conftest.py](https://github.com/raindata5/gourmand-api/blob/master/gourmandapiapptests/conftest.py) we are actually able to run alembic commands from inside our python file here is what we have there

            # alembic_cfg = Config("/mnt/c/Users/Ron/git-repos/gourmandapi/alembic2.ini")
            # alembic_cfg.set_main_option('script_location', "/mnt/c/Users/Ron/git-repos/gourmandapi/g_test_db")
            # alembic_cfg.set_main_option("sqlalchemy.url", f"postgresql://{settings.pg_oltp_api_user}:{settings.pg_oltp_api_password}@{settings.pg_oltp_api_host}/{settings.pg_oltp_api_db_test}")
            # command.downgrade(alembic_cfg, "base")
            # command.upgrade(alembic_cfg, "head")
        This permits us to teardown the database on each test and then build it up immediately\
        Tradeoff: This can make our tests take a while to run
            
        Two: Another option is to use the [gourmand-api/postgres-table-truncates.sql](https://github.com/raindata5/gourmand-api/blob/master/postgres-table-truncates.sql) file in the following manner

            ps_conn = psycopg2.connect(dbname=settings.pg_oltp_api_db_test, user=settings.pg_oltp_api_user, password=settings.pg_oltp_api_password, host= settings.pg_oltp_api_host, port=settings.pg_oltp_api_port)
            ps_cursor = ps_conn.cursor()
            sql_file = open('postgres-table-truncates.sql','r')
            ps_cursor.execute(sql_file.read())
            ps_conn.commit()
            ps_cursor.close()
            ps_conn.close()
        This permits us to simply truncate the tables which is far quicker then tearing down the database and then recreating it
        Tradeoff: Due to the Autoincrementing nature of the PKs if you do not specify the id when creating insertin certain rows then the id will continue to rise one each test. However there are simple workarounds for this that can be seen in the code.

        Either way you could always alternate between the two when needed.

        Concerning the Redis Store only one option is needed which is the following 

            for key in r1_client.scan_iter("*"):
                r1_client.delete(key)
        This will remove all the keys and their values.

    5. Also this API has a workflow with Github Actions to create a CI/CD pipeline where it automatically tests your api making sure all the tests succeed before pushing it out to production (Ubuntu VM via SSH in my case)
9. Data Analysis 
    1. As the name suggests we can actually put our data to use. First you will need to run `git clone https://github.com/raindata5/data-analysis-business-economics-policy.git` 
    2. Once in this directory you will just have to create a new virtual environment and run `pip install -r requirements.txt` (who would've thought 😅)
    3. Specifically for the data we stored in this project you can navigate to [data-analysis-business-economics-policy/gourmand_data_analysis/](https://github.com/raindata5/data-analysis-business-economics-policy/tree/master/gourmand_data_analysis) and check out the analyses carried out there.

# Where to go from here
There are some features of this main project that I still want to implement in the directly in the code as well was in the documentation. They are the following in no particular order:

- Refactor a bunch of the data pipeline code
- CI/CD Pipeline for data pipeline itself
- End-to-End test
- Web Frontend for the API using simple [HTML]()
- Web-Hosted Dashboard using [Plotly and Dash]() framework
    - For the Business data as well as the pipeline monitoring
- Dedicated Log Analysis and Storage using [ELK stack]()
    - While able to implement this on my local computer for sending the Postgres logs to ElasticSearch my current Ubuntu VM most likely dont't have enough ram for this
- Make more use of Docker
    - Currently only the API supports deployment with Docker as can be seen in it's respective repository
- Use [Apache Spark]() for transformations and possibly ingestions
- Replicate database using <a href="https://debezium.io/"> Debezium</a> and pair that with Kafka to send those to a DWH
- Add gifs to the documentation to facilate understanding
- Set up a virtual network with Azure for facilitating access to directly to database

# Contact
As I am still editing this project and it's documentation there may be some flaws noticed or any suggestions which I'm always open to 😀!\
So feel free to message me at via one of the following options :
- [GitHub](https://github.com/raindata5)
- [Twitter](https://twitter.com/RonaldD66239715)
- [Email](mailto:Ronalddavisr3@gmail.com)
- [LinkedIn](https://www.linkedin.com/in/ronald-d-011750129/)

        

     




