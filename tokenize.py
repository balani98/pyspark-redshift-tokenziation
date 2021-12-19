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
def get_referenced_filepath(file_name, matchFunc=os.path.isfile):
    for dir_name in sys.path:
        candidate = os.path.join(dir_name, file_name)
        if matchFunc(candidate):
            return candidate
    raise Exception("Can't find file: ".format(file_name))



def mask(dynamicRecord):
    config = {}
    with open(get_referenced_filepath('config.json'), "r") as f:
        config = json.load(f)
    
    columns = config["columns"]
    for col in columns:
        dynamicRecord[col]=hashlib.sha256(dynamicRecord['teacher'].encode()).hexdigest()
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
## @type: DataSource
## @args: [database = "cloudbhaidb", table_name = "students_csv", redshift_tmp_dir = args["TempDir"], transformation_ctx = "student"]


## @inputs: []
student = glueContext.create_dynamic_frame.from_catalog(database = "cloudbhaidb", table_name = "students_csv", redshift_tmp_dir = args["TempDir"], transformation_ctx = "student")
## @type: DataSource
## @args: [database = "cloudbhaidb", table_name = "class_csv", redshift_tmp_dir = args["TempDir"], transformation_ctx = "class1"]
## @return: class
## @inputs: []
class1 = glueContext.create_dynamic_frame.from_catalog(database = "cloudbhaidb", table_name = "class_csv", redshift_tmp_dir = args["TempDir"], transformation_ctx = "class1")


## @return: student## @type: Join
## @args: [keys1 = [name], keys2 = [students_name]]
## @return: <output>
## @inputs: [frame1 = student, frame2 = class1]
class1_student = Join.apply(frame1 = student, frame2 = class1, keys1 = ["name"], keys2 = ["students_name"], transformation_ctx = "class1_student")
## @type: SelectFields
## @args: [paths = ["paths"], transformation_ctx = "result"]
## @return: result
## @inputs: [frame = <frame>]
result = SelectFields.apply(frame =  class1_student, paths = ["students_name","age","address","birthday","class","teacher"], transformation_ctx = "result")

## @type: DataSink
## @args: [catalog_connection = "redshift-connection", connection_options = {"dbtable": "classdetails_csv", "database": "testdb"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = result, catalog_connection = "redshift-connection", connection_options = {"dbtable":"original.classdetails_csv", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
masked_dynamicframe = Map.apply(frame=result, f=mask)
datasink5 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = masked_dynamicframe, catalog_connection = "redshift-connection", connection_options = {"dbtable":"encrypted.classdetails_csv", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink5")
job.commit()