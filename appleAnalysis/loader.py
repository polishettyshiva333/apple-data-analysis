# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformedDF):
        self.transformedDF = transformedDF

    def sink(self):
        pass

class AirpodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type = "dbfs",
            df = self.transformedDF,
            path = "dbfs:/FileStore/tables/apple_analysis/output",
            method = "overwrite",
            #params = None 
        ).load_data_frame()

class OnlyAirpodsAndiPhoneLoader(AbstractLoader):

    def sink(self):
        params ={
            "partitionByColumns": ["location"]
        }
        get_sink_source(
            sink_type = "dbfs_partition",
            df = self.transformedDF,
            path = "dbfs:/FileStore/tables/apple_analysis/output/",
            method = "overwrite", 
            params = params
        ).load_data_frame()  

        get_sink_source(
            sink_type = "delta",
            df = self.transformedDF,
            path = "default.onlyAirpodsAndiPhone",
            method = "overwrite", 
            
        ).load_data_frame()  
        
             