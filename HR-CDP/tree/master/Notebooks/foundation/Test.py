# Databricks notebook source
# MAGIC %run ./common_functions

# COMMAND ----------

#sqlContext.sql('refresh table u_hrcdp_core_nonconf.batchlog')

#update_batchlog(38,'WKDAY', 'SUPORG', 'WRANGLE + UNIFY', 796, 0, 0, 57, None, None, None, 'FAILURE', "Testing")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from 

# COMMAND ----------



# COMMAND ----------

root_path = '/data/trusted/hr-csi/common/global/hrcdp'

source_db = 'f_hr_cdp_workday'
source_table = 'workday_workloc'
source_table_pk = 'LOCATION_ID'
source_table_key = 'PK_WORKLOC_KEY'

target_db = 't_hr_cdp_csi'
target_table = 't_workloc'
temp_write_table = 't_worklocation'
exception_table = 't_workloc_excp'
unify_exception_table = 'workloc_excp'
target_db_location = root_path + "/" + target_db
target_table_location = target_db_location + "/" + target_table

unified_table = 'workloc'
unified_db = 'u_hrcdp_core_csi'
unified_db_location = '/data/unified/hr-csi/common/hrcdp/u_hrcdp_core_csi'
unified_table_location = unified_db_location+ "/" + unified_table



exception_table_location =target_db_location + "/" + exception_table
global_mapping_table = 'workloc_gobalmapping_test'
global_cities_table = 'external_gobalcities_test'

root_path_batchlog_table = '/data/unified/hr-csi/common/hrcdp'
batchlog_db = 'u_hrcdp_core_nonconf'
batchlog_table = 'batchlog'

# The below parameters are used to call the update_batchlog function
batch_id = 0
batch_source = 'WKDAY'
batch_integration_name = 'WORKLOC'
batch_layer = 'WRANGLE + UNIFY'
batch_notebook = 'wrangle_workloc'
batch_extract_filename = 'workday_workoc' # To be implemented
batch_extract_timestamp = sqlContext.sql('select distinct to_timestamp(ingest_ts,"yyyy-MM-dd_HH-mm-ss") as ingest_ts \
from {0}.{1} order by ingest_ts desc limit 1'.format(source_db,source_table)).first()[0]

print (batch_extract_timestamp)

batch_start_timestamp = get_currenttimestamp()
batch_end_timestamp = None # to be calculated at each call to the function
batch_total_rows_from_source = None 
batch_total_rows_rejected_rawlayer = None
batch_total_rows_rejected_wranglelayer = None
batch_total_rows_with_dataquality_issues = None
batch_dw_records_updated = None
batch_dw_newrecords_inserted = None
batch_records_ignored = None
batch_status = BATCH_FAILURE
batch_error_on_termination = None

# Create a new entry for this execution in batclog table



sqlContext.sql('Refresh table {0}.{1}'.format(batchlog_db, batchlog_table))

print ('hello')

batch_id = create_batchlog(batch_source,batch_integration_name,batch_layer,batch_notebook, \
 batch_extract_filename,batch_extract_timestamp,batch_start_timestamp) 

print (batch_id)

# COMMAND ----------

update_batchlog(1,'WKDAY','WORKLOC','WRANGLE + UNIFY',796,0,0,57,None,None,None,BATCH_FAILURE,'TESTING')

#1 WKDAY WORKLOC WRANGLE + UNIFY 796 0 0 57 None None None FAILURE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC refresh table u_hrcdp_core_nonconf.batchlog 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from u_hrcdp_core_nonconf.batchlog order by BATCH_ID DESC, batch_start_timestamp DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from f_hr_cdp_workday.workday_workloc

# COMMAND ----------

valuesA = [('Pirate',1),('Monkey',2),('Ninja',3),('Spaghetti',4)]
TableA = spark.createDataFrame(valuesA,['name','id'])
 
valuesB = [('Rutabaga',1),('Pirate',2),('Ninja',3),('Darth Vader',4)]
TableB = spark.createDataFrame(valuesB,['name','id'])
 
TableA.show()
TableB.show()

# COMMAND ----------

df1 = TableA.select("name")
display (df1)

# COMMAND ----------

inner_join = TableA.join(TableB, TableA.name == TableB.name, how = 'left')
display(inner_join)

# COMMAND ----------

source_db = 'f_hr_cdp_workday'
source_table = 'workday_workloc'

batch_extract_timestamp = sqlContext.sql('select distinct to_timestamp(ingest_ts,"yyyy-MM-dd_HH-mm-ss") as ingest_ts \
from {0}.{1} order by ingest_ts desc limit 1'.format(source_db,source_table)).first()[0]

print (batch_extract_timestamp)

# COMMAND ----------

#update_batchlog(batch_id,batch_source,batch_integration_name,batch_layer,total_rows_from_source,total_rows_rejected_rawlayer,total_rows_rejected_wranglelayer,total_rows_with_dataquality_issues,dw_records_updated,dw_newrecords_inserted,records_ignored,batch_status,error_on_termination)

#update_batchlog(1,'WKDAY','WORKLOC','WRANGLE + UNIFY',796,None,None,None,None,None,None,BATCH_FAILURE,'TESTING')

#1 WKDAY WORKLOC WRANGLE + UNIFY 796 0 0 57 None None None FAILURE

batch_id = 1
batch_source = 'WKDAY'
batch_integration_name = 'WORKLOC'
batch_layer = 'WRANGLE + UNIFY'
total_rows_from_source = 796
total_rows_rejected_rawlayer = 0
total_rows_rejected_wranglelayer = 0
total_rows_with_dataquality_issues = 57
dw_records_updated = None
dw_newrecords_inserted = None
records_ignored = None
batch_status = 'FAILURE'
error_on_termination = 'TESTING'

#batch_start_timestamp = get_currenttimestamp()

batchlog_db = 'u_hrcdp_core_nonconf'
batchlog_db_location = 'adl://us6datahubadlsprod001.azuredatalakestore.net/data/unified/hr-nonconf/common/hrcdp/u_hrcdp_core_nonconf'
batchlog_table = 'batchlog'
temp_batchlog_table = 'batchlog_temp'
batchlog_table_location = batchlog_db_location + '/' + batchlog_table
temp_batchlog_table_location = batchlog_db_location + '/' + temp_batchlog_table   
    
# Create a dummy table to serve as source for the create table statement for the BatchLog table
trusted_db = 't_hr_cdp_nonconf'
trusted_db_location = 'adl://us6datahubadlsprod001.azuredatalakestore.net/data/trusted/hr-nonconf/common/global/hrcdp/t_hr_cdp_nonconf'
    
sqlContext.sql('use {0}'.format(batchlog_db))

#print ("hello")

batchlog_matchrecDF = sqlContext.sql('select * from batchlog where batch_id = {0} and source = "{1}" and integration_name = "{2}" and layer = "{3}" order by batch_record_lastupdated_timestamp desc limit 1'.format(batch_id,batch_source,batch_integration_name,batch_layer))

#print (batch_id)
#batchlog_matchrecDF = sqlContext.sql('select * from batchlog where source = "{0}" and integration_name = "{1}" and layer = "{2}" and notebook = "{3}" and extract_filename = "{4}" order by batch_id desc limit 1'.format(source,integration_name,layer,notebook,extract_filename))

if batchlog_matchrecDF.count() == 0: 
    sqlContext.sql('Refresh table {0}.{1}'.format(batchlog_db, batchlog_table))
    dbutils.notebook.exit('a')    # Return False (Failure) as the input batch_id was not found in batchlog table

 #print ("hello found - rec")
#batchlog_newdataDF1 = batchlog_matchrecDF

if batch_status == 'IN PROGRESS':
    batch_end_timestamp = None
else :
    batch_end_timestamp = get_currenttimestamp()

batchlog_matchrec_list = batchlog_matchrecDF.select("source","integration_name","layer","notebook","extract_filename","extract_timestamp","batch_start_timestamp","batch_record_created_timestamp").collect()

for r in batchlog_matchrec_list:
  source = r[0]
  integration_name = r[1]
  layer = r[2]
  notebook = r[3]
  extract_filename = r[4]
  extract_timestamp = r[5]
  batch_start_timestamp = r[6]
  batch_record_created_timestamp = r[7]
  
#display (batchlog_matchrec_list)

batchlog_newdataDF =       spark.createDataFrame([(batch_id,source,integration_name,layer,notebook,extract_filename,extract_timestamp,batch_start_timestamp,batch_end_timestamp,total_rows_from_source,total_rows_rejected_rawlayer,total_rows_rejected_wranglelayer,total_rows_with_dataquality_issues,dw_records_updated,dw_newrecords_inserted,records_ignored,batch_status,error_on_termination,batch_record_created_timestamp,get_currenttimestamp())],batch_schema)
    
# batchlog_matchrec_list[7]
batchlog_alldataDF = sqlContext.sql('select * from batchlog')

#  Take all data in batchlog table and replace the existing batch record with the new one    
   
batchlog_tempDF = batchlog_alldataDF.exceptAll(batchlog_matchrecDF).unionByName(batchlog_newdataDF).dropDuplicates()

# Force write to disk to avoid the "Cannot write to same table that is being read from" error
    
batchlog_tempDF.write.format('parquet').mode("overwrite").saveAsTable(temp_batchlog_table)
      
sqlContext.sql('Refresh table {0}.{1}'.format(batchlog_db, temp_batchlog_table))
      
# Create new dataframe from temp table & overwrite batchlog table to avoid "Cannot write to same table that is being read from" error

print ('Trying to Read temp batchlog')

batchlog_FinalDF = sqlContext.sql('select * from {0}'.format(temp_batchlog_table))

batchlog_FinalDF.write.format('parquet').mode("overwrite").saveAsTable(batchlog_table)
    
sqlContext.sql('Refresh table {0}.{1}'.format(batchlog_db, batchlog_table))
    
# Drop the temporary table
      
sqlContext.sql('Drop table {0}.{1}'.format(batchlog_db, temp_batchlog_table))

sqlContext.sql('Refresh table {0}.{1}'.format(batchlog_db, batchlog_table))

#return(True) # Return True to indicate that procedure completed successfully