# Johnson&Johnson Data Engineering Adventure
This repository is for the data engineering technical assessment, created &amp; owned by JnJ
--------------------------------------------------------------------------------------------
Welcome Onboard, it is good to have you join our Data Analytics Team. Get ready to dive deep into the world of data, specifically Data Engineering, where our job is to take care of a huge amount to data and provide it to be analytics ready. We clean & transform data from different sources and construct analytic-ready fact tables for our fellow Data Scientists, Data Analysts, & BI Engineers to use to come up with new insights.

Technologies used for the task
------------------------------
* PySpark (Python API for Apache Spark)
* SQL
* Git & GitHub
* Databricks
* pytest (Python testing framework)

Steps To Run The Pipeline
-------------------------
* Create a Databricks account
* Integrate your Databricks account with our Github repository
* Pull the repo to your Databricks Workspace
* Create a new Databricks Job
* Create a new Notebook for unit testing & use Task-2-Solution.pdf file as guidance to create it
* Add a new task to the job based on the notebook created in the previous step (task type = Notebook)
* Add a new task to the job based on pipeline.py Python script in the PySpark folder in our repository (task type = Python script)
* (Optional) add a new task based on a new Databricks notebook to display the results
* Now you have three tasks in a single Databricks job, creating a simple data pipeline (unit testing -> data processing -> data visualization)
* Run the job & make sure the pipeline is working

Assumptions
-----------
1) Data was previously ingested from an external source (ex. Snowflake or Drive)
2) Results were published for consumption to an external destination (ex. Snowflake)

Note: we sticked on using Databricks volumes for storing our input & output files for simplicity.



