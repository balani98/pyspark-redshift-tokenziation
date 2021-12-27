
# Here we have tried to crawl data directly from S3 with catalog 
# But we were not able to perform joins and selection on Dymanic frames here so we have done on spark dataframes itself
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import hashlib
import json
import os
import sys
from awsglue.dynamicframe import DynamicFrame
def get_referenced_filepath(file_name, matchFunc=os.path.isfile):
    for dir_name in sys.path:
        candidate = os.path.join(dir_name, file_name)
        if matchFunc(candidate):
            return candidate
    raise Exception("Can't find file: ".format(file_name))



def mask(dynamicRecord):
    config = {}
    # remember to add file in referenced paths
    with open(get_referenced_filepath('config.json'), "r") as f:
        config = json.load(f)
    
    columns = config["columns"]
    for col in columns:
        dynamicRecord[col]=hashlib.sha256(dynamicRecord[col].encode()).hexdigest()
    #dynamicRecord['teacher'] =  hashlib.sha256(dynamicRecord['teacher'].encode()).hexdigest()
    #dynamicRecord['class'] =  "*****************"
    #dynamicRecord['birthday'] = hashlib.sha256(dynamicRecord['birthday'].encode()).hexdigest()

    return dynamicRecord


## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @inputs: []
# getting the data from S3 for student dataframe
student = glueContext.create_dynamic_frame_from_options("s3", {'paths': ["s3://cloudbhai2/students.csv"] }, format="csv",format_options={
        "withHeader": True,
        "separator": ","
    } )
# converting to spark Dataframe
studentDF = student.toDF()
# getting the data from S3 for class dataframe
class1 = glueContext.create_dynamic_frame_from_options("s3", {'paths': ["s3://cloudbhai2/class.csv"] }, format="csv",format_options={
        "withHeader": True,
        "separator": ","
    } )
# converting to spark dataframe
class1DF = class1.toDF()
#Joining the dataframes 
class1_studentDF =   studentDF.join(class1DF, studentDF['name'] == class1DF['students_name'] , how='inner')
# selecting the columns
select_results = class1_studentDF.select("students_name","age","address","birthday","class","teacher","character")
# converting to Dyanamic dataframes
class1_studentDDF = DynamicFrame.fromDF(select_results,glueContext,"class1_studentDDF")
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = class1_studentDDF, catalog_connection = "redshift-connection", connection_options = {"dbtable":"original.classdetails_csv", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
# masking the data from DDF
masked_dynamicframe = Map.apply(frame=class1_studentDDF, f=mask)
# writing the masked DDF to redshift
datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = masked_dynamicframe, catalog_connection = "redshift-connection", connection_options = {"dbtable":"encrypted.classdetails_csv", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
job.commit()