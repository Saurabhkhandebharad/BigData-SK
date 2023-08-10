# Importing required modules from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Creating a Spark session
spark = SparkSession.builder.appName("Load Data from S3").getOrCreate()

# Loading the data from an S3 bucket into a DataFrame
df = spark.read.format("csv").option("header", "true").load("s3://gauravproject/kaggle/2019-Nov.csv")

'''
Schema before transformation - no. of features = 9
Event Time, Event Type, Product_ID, Category_ID, Category_Code, brand, price, User_Id, User_Session

'''

# Discovering that a significant number of rows have missing values in both 'category_code' and 'brand', I opted to remove these specific rows for further analysis.
# Filtering out rows where both 'category_code' and 'brand' are null
df2 = df.filter((col("category_code").isNull()) & (col("brand").isNull()))
# That way, we have atleast one of the column indicating the identity of the product.

# Creating a new DataFrame without rows from df2
dfsuper = df.subtract(df2).dropna(how='all')

# Splitting the 'category_code' column to extract 'category' and 'subcategory'
dfsuper = dfsuper.withColumn('category', split('category_code', '\.')[0]) \
                  .withColumn('subcategory', split('category_code', '\.')[1])

# Splitting the 'event_time' column to extract 'event_date', 'event_time(UTC)', and 'garbage'
dfsuper = dfsuper.withColumn('event_date', split('event_time', ' ')[0]) \
                  .withColumn('event_time(UTC)', split('event_time', ' ')[1]) \
                  .withColumn('garbage', split('event_time', ' ')[2])

# Dropping unnecessary columns
dfsuper = dfsuper.drop("event_time", "category_code", "garbage")

# Pivoting the DataFrame to reshape it for analysis
pivot_df = dfsuper.groupBy("event_type", "product_id", "category_id", "brand", "price", "user_id", "user_session", "category", "subcategory",  "event_date", "event_time(UTC)")\
    .pivot("event_type", ["cart", "view", "purchase"])\
    .agg(count("*"))\
    .fillna(0)

# Filtering out rows where 'price' is not equal to 0
dffinal = pivot_df.filter(col("price") != 0)

# Dropping rows with null values in the 'price' column
dffinal = dffinal.na.drop(subset=["price"])

# Casting columns to their appropriate data types
dffinal = dffinal.withColumn("product_id", col("product_id").cast(LongType())) \
    .withColumn("category_id", col("category_id").cast(LongType())) \
    .withColumn("user_id", col("user_id").cast(LongType())) \
    .withColumn("price", col("price").cast(FloatType())) \
    .withColumn("cart", col("cart").cast(IntegerType())) \
    .withColumn("view", col("view").cast(IntegerType())) \
    .withColumn("purchase", col("purchase").cast(IntegerType())) \
    .withColumn("event_date", col("event_date").cast(DateType())) \
    .withColumn("event_time(UTC)", col("event_time(UTC)").cast(TimestampType()))

# Splitting 'event_time(UTC)' to extract 'time' and creating 'time_int'
df_1 = dffinal.withColumn('date', split('event_time(UTC)', ' ')[0]) \
       .withColumn('time', split('event_time(UTC)', ' ')[1])
       
# Dropping the 'date' column
df_1 = df_1.drop("date")

# Filling null values with 'not specified'
df_1 = df_1.fillna('not specified')

# Converting 'time' to integer representation and creating 'time_new'
df_2 = df_1.withColumn('time_int', unix_timestamp('time', 'HH:mm:ss').cast('integer'))
df_2 = df_2.withColumn('time_new', from_unixtime('time_int', 'HH:mm:ss'))

'''
Schema after transformation - no. of features = 14
Event Type, Product_ID, Category_ID, brand, price, User_Id, User_Session,  Category, Subcategory,Event_Date,
Event_Time(UTC), Cart, View, Purchase

'''

# Writing the processed DataFrame to Parquet format in an S3 bucket
df_2.write.format("parquet").save("s3://ecomproject2/processed_again/newnov19.parquet")
