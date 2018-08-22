//SPARK SQL Window Aggregation Functions
// Window aggregate functions are the functions that performs a calculation over a group of record call window
spark-shell --packages com.databricks:spark-csv_2.11:1.5.0 
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("dataset/retailProductData1.csv")

// Rank
// import Window expression
import org.apache.spark.sql.expressions.Window 
// create a Window specification / Partition. Mention column to be partitioned and ordered
val categoryBucket = Window.partitionBy("category").orderBy("revenue") 
// create a new rank column and apply ranking function with window specification / Partition
// rank will generate 1,1,3,4
val rankProductsByCategory = df.withColumn("rank",rank over categoryBucket) 
// dense_rank will generate ranks 1,1,2,3
val denseRankProductsByCategory = df.withColumn("dense_rank",dense_rank over categoryBucket) 

//percent_rank
val percentRankProductsByCategory = df.withColumn("percent_rank",percent_rank over categoryBucket)

//ntile computes the ntile group id (from 1 to n inclusive) in an ordered window partition.
val nTileProductsByCategory = df.select('*,ntile(3) over categoryBucket as "ntile")  '

//row_number returns a sequential number starting at 1 within a window partition.
val rowNumByProductsCategory = df.withColumn("row_number", row_number() over categoryBucket)