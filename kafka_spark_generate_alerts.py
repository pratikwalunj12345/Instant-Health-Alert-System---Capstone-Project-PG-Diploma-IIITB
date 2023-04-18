from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
# create spark session
spark = SparkSession.builder.appName("PatientVitalInfo").enableHiveSupport().getOrCreate()

#Define schema 
schema = StructType() \
         .add("customerId", IntegerType()) \
         .add("bp", IntegerType()) \
         .add("heartBeat", IntegerType()) \
         .add("message_time", TimestampType()) \

# Read Patients Vital Info
patient_vital_info  = spark.readStream \
                           .format("parquet") \
                           .option("maxFilesPerTrigger","1") \
                           .schema(schema) \
                           .load("/user/hadoop/Patients-vital-info/")

# read patient contact info table from hive
patient_contact_info = spark.sql("SELECT * FROM patient_contact_info")

# read threshold reference table from hive
threshold_reference_table = spark.sql("select * from threshold_reference_table")

patient_complete_data = patient_vital_info.join(patient_contact_info, patient_vital_info.customerId == patient_contact_info.patientid, 'left_outer')


patient_complete_data.registerTempTable("patient_complete_data_tbl")

bp = spark.sql("select a.patientname,a.age,a.patientaddress,a.phone_number,a.admitted_ward,a.bp,a.heartBeat,a.message_time,b.alert_message from patient_complete_data_tbl a, threshold_reference_table b where b.attribute = 'bp' and (a.age>=b.low_age_limit and a.age<=b.high_age_limit) and (a.bp>=b.low_value_limit and a.bp<=b.high_value_limit) and b.alert_flag = 1")

heartBeat = spark.sql("select a.patientname,a.age,a.patientaddress,a.phone_number,a.admitted_ward,a.bp,a.heartBeat,a.message_time,b.alert_message from patient_complete_data_tbl a,threshold_reference_table b where b.attribute = 'heartBeat' and (a.age>=b.low_age_limit and a.age<=b.high_age_limit) and (a.heartBeat>=b.low_value_limit and a.heartBeat<=b.high_value_limit) and b.alert_flag = 1")

alert_df = bp.union(heartBeat).withColumnRenamed("message_time","input_message_time")

alert_final_df = alert_df.selectExpr("to_json(struct(*)) AS value")

#Final output 
output= alert_final_df \
        .writeStream  \
        .outputMode("append")  \
        .format("kafka")  \
        .option("kafka.bootstrap.servers","3.211.253.82:9092")  \
        .option("topic",message-alert)  \
        .option("checkpointLocation","/user/hadoop/Doctor_Queue_cp1")  \
        .start()


output.awaitTermination()