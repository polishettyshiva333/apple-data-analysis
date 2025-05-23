# Databricks notebook source
# MAGIC %run "./reader_factory"

# COMMAND ----------

class Extractor:
    """
    Abstract Class
    """

    def __init__(self):
        pass
    def extract(self):
        pass

class AirpodsAfterIphoneExtractor(Extractor):

    def extract(self):
        """
        Implement the steps for extracting or reading the data
        """
        transactionInputDF = get_data_source(
            data_type = "csv",
            file_path = "/mnt/appleAnalysis/Transaction_Updated.csv"
        ).get_data_frame()

        transactionInputDF.orderBy("customer_id", "transaction_date").show()

        customerInputDF = get_data_source(
            data_type = "delta",
            file_path = "default.customer_delta_table"
        ).get_data_frame()    

        customerInputDF.show()


        inputDFs = {
            "transactionInputDF": transactionInputDF,
            "customerInputDF": customerInputDF
        }    

        return inputDFs