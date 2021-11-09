from pyspark.sql import SparkSession


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
import configparser

# Initiating spark session
spark = SparkSession.builder.appName("DailyDataIngestAndRefine").master("local[2]").getOrCreate()

# Reading the config Parser
config = configparser.ConfigParser()
config.read(r'../projectconfigs/config.ini')
inputLocation = config.get('paths', 'inputLocation')

# Reading landing zone

landingFileSchema = StructType([
    StructField('Sale_ID', StringType(), True),
    StructField('Product_ID', StringType(), True),
    StructField('Quantity_Sold', IntegerType(), True),
    StructField('Vendor_ID', StringType(), True),
    StructField('Sale_Date', TimestampType(), True),
    StructField('Sale_Amount', DoubleType(), True),
    StructField('Sale_Currency', StringType(), True),
])

landingFileDF = spark.read.schema(landingFileSchema).option("delimiter", "|")\
    .csv(inputLocation)


landingFileDF.show()
