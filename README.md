# Museum Visitor Kiosk Pipeline

#### This project aims to extract data from Liverpool Museum of Natural History, which include visitor satisfaction kiosk data as well as exhibition data. Then, a database is set up to hold this data, whilst ensuring scalability with new data as well as handling anomalies. The data should be moved to the database, from which analysis can be done on the data to discover trends and potential improvements with the aim of providing greater value to the Liverpool community and the wider public. 

## Files:

* **extract.py**: connects to an S3 and downloads relevant files
* **schema.sql**: creates required database tables and seeds them with initial data
* **pipeline.py**: downloads kiosk data from S3 and uploads it to the database
* **kiosk_analysis.ipynb**: connects to, and explores the database

## Data

The data for the LMNH includes kiosk output .csv files (lmnh_hist_data_X.csv),  
As well as details of specific exhibits within .json files (lmnh_exhibition_XXXXX.json).

The extract.py file handles the combination of multiple sources of kiosk data.

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
LMNH has no current plans to change the number of buttons, or the meaning of each button, but would like the option to do so easily in the future. Your eventual data storage solution should be flexible enough to accommodate this.