# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast, collect_set, size, array_contains

class Transformer:
    def __init__(self):
        pass

    def transform(self, inputDFs):
        pass

class AirpodsAfteriPhoneTransformer(Transformer):

        def transform(self, inputDFs):
            """
                    # Customers who have bought Airpods after iPhone
            """

            transactionInputDF = inputDFs.get("transactionInputDF")

            print("transactionInputDF in transform")

            transactionInputDF.show()

            windowSpec = Window.partitionBy("customer_id").orderBy("transaction_date")

            transformedDF = transactionInputDF.withColumn(
                "next_product_name", lead("product_name").over(windowSpec)
            )

            print("Airpods after Buying iPhone")
            transformedDF.orderBy("customer_id", "transaction_date", "product_name").show()

            filteredDF = transformedDF.filter(
                (col("product_name") == "iPhone") & 
                (col("next_product_name") == "AirPods")
                )
        
            filteredDF.orderBy("customer_id", "transaction_date", "product_name").show()

            customerInputDF = inputDFs.get("customerInputDF")

            customerInputDF.show()

            joinDF = customerInputDF.join(
                broadcast(filteredDF),
                "customer_id"
                )
            
            print("Joined DF")
            joinDF.show()

            return joinDF.select(
                "customer_id",
                "customer_name",
                "location"
                )


class OnlyAirpodsAndiPhone(Transformer):

    def transform(self, inputDFs):
        """
        Customer who have bought only iPhone and Airpods nothing else
        """

        transactionInputDF = inputDFs.get("transactionInputDF")

        print("transactionInputDF in transform")

        groupedDF = transactionInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("product")
        )

        print("Grouped Df")
        groupedDF.show()

        filteredDF = groupedDF.filter(
                (array_contains(col("product"), "iPhone")) &
                (array_contains(col("product"), "AirPods"))&
                (size(col("product")) == 2)
                )
        print("Only airpods and iPhone")
        filteredDF.show()

        customerInputDF = inputDFs.get("customerInputDF")

        customerInputDF.show()

        joinDF = customerInputDF.join(
            broadcast(filteredDF),
            "customer_id"
            )
            
        print("Joined DF")
        joinDF.show()

        return joinDF.select(
            "customer_id",
            "customer_name",
            "location"
            )

