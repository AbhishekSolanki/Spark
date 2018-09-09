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
val nTileProductsByCategory = df.select('*,ntile(3) over categoryBucket as "ntile")    '

//row_number returns a sequential number starting at 1 within a window partition.
val rowNumByProductsCategory = df.withColumn("row_number", row_number() over categoryBucket)

// Analytic Functions
val cumeDistProductsCatregory = df.withColumn("cume_dist", cume_dist() over categoryBucket)
// lag function
//Lag function allows us to compare current row with preceding rows within each partition depending on the second argument (offset) which is by default set to 1 i.e. 
//previous row but you can change that parameter 2 to compare against every other preceding row.
//The 3rd parameter is default value to be returned when no preceding values exists or null.
val lagProducstCategory = df.withColumn("lag", lag('revenue, 1) over windowSpec)   '

//lead function
//Lead function allows us to compare current row with subsequent rows within each partition depending on the second argument (offset) which is by default set to 1 i.e. n
//ext row but you can change that parameter 2 to compare against every other row.
//The 3rd parameter is default value to be returned when no subsequent values exists or null.
val leadProducstCategory = df.withColumn("lag", lead('revenue, 1) over windowSpec) 
