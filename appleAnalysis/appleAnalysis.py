# Databricks notebook source
# MAGIC %run "./transform"

# COMMAND ----------

# MAGIC %run "./extractor"

# COMMAND ----------

# MAGIC %run "./loader"

# COMMAND ----------

class FirstWorkFlow:
    """
    ETL pipeline to generate the data for all customers who have bought Airpods after buying iPhone
    """

    def __init__(self):
        pass
    def runner(self):

        #Step 1: Extract all required data from different sources
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        #Step 2: Implement the transforamtion logic 
        #Customers who have bought Airpods after iPhone
        transformedDF = AirpodsAfteriPhoneTransformer().transform(
            inputDFs
            )
        
        #Step 3: Load all required data to different sink
        AirpodsAfterIphoneLoader(transformedDF).sink()






# COMMAND ----------

class SecondWorkFlow:
    """
    ETL pipeline to generate the data for all customers who have bought only iPhone and Airpods
    """

    def __init__(self):
        pass
    def runner(self):

        #Step 1: Extract all required data from different sources
        inputDFs = AirpodsAfterIphoneExtractor().extract()

        #Step 2: Implement the transforamtion logic 
        #Customers who have bought Airpods after iPhone
        onlyAirpodsAndiPhoneDF = OnlyAirpodsAndiPhone().transform(
            inputDFs
            )
        
        #Step 3: Load all required data to different sink
        OnlyAirpodsAndiPhoneLoader(onlyAirpodsAndiPhoneDF).sink()






# COMMAND ----------

class WorkFlowRunner:
    
    def __init__(self, name):
        self.name = name

    def runner(self):
        if self.name == "FirstWorkFlow":
            return FirstWorkFlow().runner()
        elif self.name == "SecondWorkFlow":
            return SecondWorkFlow().runner()
        else:
            raise ValueError("Not Implemened for {self.name}")
        
name = "SecondWorkFlow"

workFlowRunner = WorkFlowRunner(name).runner() 

# COMMAND ----------

#display(dbutils.fs.mounts())

#display(dbutils.fs.rm("/FileStore/tables/apple_analysis/output/", recurse=True))

display(dbutils.fs.ls("/FileStore/tables/apple_analysis/output/location=Colorado"))

df = spark.read.format("delta").load("dbfs:/FileStore/tables/apple_analysis/output/")

df.show()