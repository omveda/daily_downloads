# daily_downloads
Ingest daily download data and insert into MapD table for analysis

On Hoarder
------------
Run create_last_execution.py to create an empty file whose mtime will be used to get newer CSV files.
Execute get_ce_download_data.py which will perform all the tasks:
  - Grab all the newly created CSV files
  - copy these new CSVs to the AWS helper system
  - update the last execution time stamp
  - execute daily.py on the helper system
  
On Helper System
----------------
The script daily.py will perform the following tasks:
  - Coalesce all the CSVs into a Pandas dataframe
  - Add new columns and rename existing columns
  - Find the lat/lon information for each download site 
  - Find the domain name for each download site
  - Interpret information for each column based on the downloaded image (gpu/cpu, image format etc)
  - write the combined dataframe to a CSV
  - copy CSV to S3
  - use Pymapd to update table with the CSV from S3
 
