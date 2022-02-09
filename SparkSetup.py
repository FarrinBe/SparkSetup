import pyspark
import os
from pyspark.sql import SparkSession 
# Setting up environment variables for Python used by PySpark Driver and Workers
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"  
# PySpark Driver
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/anaconda/bin/python" 
# PySpark Workers 
class SparkSetup:     
  def __init__(self, app_name:str='CAI_Default'):        
    self.app_name = app_name        
    self.conf = None        
    self.spark = None     
  
  def setup_configuration(self):        
      # we can source these parameters from a configuration file in future developments        
      self.conf = pyspark.SparkConf().setAll([        
        ('spark.app.name', self.app_name),
        ('spark.port.maxRetries', '500'),  # maxReties needs to be always increased to 500 to avoid network issues        
        ('spark.network.timeout', '1200'), # when there might be delays between two jobs it can help to increase this parameter (default is 120 sec)
        ('spark.task.maxFailures', '10'), # when there is incorrect setup of Spark for the problem at hand increasing this parameter can help (default is 4 attempts) 
        ('spark.executor.cores', '8'),        
        ('spark.executor.memory', '16g'),        
        ('spark.driver.memory','16g')        ])        
      return True
    
    def get_spark_session(self):        
      # Creating a unique Spark Session that can be passed to multiple computations        
      # Could also decide to create and use several Spark Sessions in parallel using several SparkSetup objects        
      self.setup_configuration()        
      self.spark = SparkSession.builder.config(conf=self.conf).enableHiveSupport().getOrCreate()  # Creating a new Spark Session using configuation above, no need to stop the Spark Context and recreate it                
      sc = self.spark.sparkContext        
      sc.setLogLevel('WARN')  # Setting log level to Warning and Errors. INFO level would be overflowing                
      return self.spark
