# Instant-Health-Alert-System---Capstone-Project-PG-Diploma-IIITB

Problem Statement

Healthcare monitoring at hospitals has witnessed new challenges. In view of these challenges, this use case proposes a reliable data pipeline solution to store and analyse a stream of real-time data flowing from various IoT devices at hospitals and health centres. 

 

The advent of IoT devices has digitised vital health data such as body temperature, heartbeat, blood pressure (BP) and more. Capturing this high-velocity stream of data and analysing these in real-time with minimal error is only possible with the use of a robust data platform and components. 

 

This project will test your knowledge of the various tools related to data pipelining, which you learnt about throughout this course. The project revolves around Apache Sqoop, Apache PySpark, Apache Kafka, Hive, HBase, which are some of the most widely used tools in the industry.

 

Your task in this project will be to build a solution to cater to the following requirements:

 

Real-time alert notification system - This use case will simulate the streaming vital data of patients and building of a data pipeline to monitor vital data to alert the subscribers in real-time based on reference data. If the vital information coming from the patient is out of the normal threshold range, an alert notification will have to be sent immediately to the registered Email-ID.

 

At the industry level, a pipeline similar to the one showed below is used for this purpose.

Broadly, you will be performing the following tasks in this project:

Taking streaming data (patients vital information) and storing it in a table

Taking batch data (patients contact information) and storing it in a table

Comparing the vital information with threshold information and analysing

Sending notifications if the data is out of the threshold limits
