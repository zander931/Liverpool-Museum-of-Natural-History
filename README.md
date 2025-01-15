# Museum Visitor Kiosk Pipeline

This project aims to extract data from the **Liverpool Museum of Natural History**, which includes transactional _visitor satisfaction kiosk data_ as well as master _exhibition data_.   
A database is set up to hold this data, whilst ensuring scalability with new data as well as handling anomalies. The data should be moved to the database, from which analysis can be done on the data to discover trends and potential improvements with the aim of providing greater value to the Liverpool community and the wider public. 

## Files:

* **schema.sql**: Creates required database tables and seeds them with initial data
* **extract.py**: Functions to connect to an S3 and download relevant files
* **transform.py**: Functions to transform the data prior to uploading
* **load.py**: Functions to load csv data, and upload transformed data
* **pipeline.py**: Manages the ETL process from extraction from S3 to uploading to the database
* **consumer.py**: Manages the ETL process from extraction from Kafka stream to uploading to the database
* **analysis.ipynb**: connects to, and explores the database for data analysis on the kiosk and exhibition data

## Data

The data for the LMNH includes kiosk output .csv files (lmnh_hist_data_X.csv),  
As well as details of specific exhibits within .json files (lmnh_exhibition_XXXXX.json).

The extract.py file handles the combination of multiple sources of kiosk data.

The data is transformed to map to the foreign keys (exhibit_id, request_id/rating_id) linked to the kiosk transaction data, hosted within the request_interaction/rating_interaction tables, respective to the type of data.

## Database Model

![Museum ERD](https://github.com/zander931/Liverpool-Museum-of-Natural-History/blob/main/museum_erd.png?raw=true)

Above is the Entity Relationship Diagram for the museum database schema.

### Ratings

While ratings are stored numerically, each one does have an associated description:

0: Terrible  
1: Bad  
2: Neutral  
3: Good  
4: Amazing  

This program is able to flexibly scale and adapt to new information if added/changed in the future.
LMNH has no current plans to change the number of buttons, or the meaning of each button, but would like the option to do so easily in the future.  
The master data can be modified within the schema data, which is idempotent.

## Running the file

The process is centrally controlled via the _pipeline.py_ script. The following should display optional options to control the output of the process, including the **s3 bucket name**, the **number of rows** uploaded to the database from the first line of the kiosk csv file data, and finally **where the output should be logged**. 

```bash
python pipeline --help
```

**Connecting** to the remote database should be simple with a '.env' file, containing sensitive login credentials for the Amazon RDS.  
```bash
bash connect.sh
```
Similarly, **resetting** the database is as simple as running the command below:
```bash
bash reset.sh schema.sql
```

### .env file requirements

An .env file is required to hold sensitive configuration information to connect to the S3 bucket that stores the exhibition and kiosk data. The .env file should also hold to configuration details to connect to the database, as well as to the Kafka stream of incoming data.

For connecting to the S3 bucket, you will require:
 - **AWS_ACCESS_KEY**
 - **AWS_SECRET_ACCESS_KEY**

For connecting to the RDS database, you will require:
 - **DB_HOST**
 - **DB_NAME**
 - **DB_PORT**
 - **DB_USER**
 - **DB_PASS**

For connecting to the Kafka stream, you will require:
 - **BOOTSTRAP_SERVERS**
 - **SECURITY_PROTOCOL**
 - **SASL_MECHANISM**
 - **USERNAME**
 - **PASSWORD**
 - **TOPIC**

## Database Views

Some views have been pre-instantiated for ease of querying the database. These views include:
 - **exhibition_info**  
 - **request_info**  
 - **rating_info**  

 You can view these views within the schema.sql file.