//spark sql window and analytics funcion
// dataset: https://github.com/AbhishekSolanki/Spark/tree/master/dataset/retailProductData1.csv
spark-shell --packages com.databricks:spark-csv_2.11:1.5.0 
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("dataset/retailProductData1.csv")

// Rank
// import Window expression
import org.apache.spark.sql.expressions.Window 
// create a Window specification. Mention parition and ordering
val categoryBucket = Window.partitionBy("category").orderBy("revenue") 
// create a new rank column and apply ranking function with window specification
// rank will generate 1,1,3,4
val rankedProductsByCategoryRevenue = df.withColumn("rank",rank over categoryBucket) 
// dense_rank will generate ranks 1,1,2,3
val rankedProductsByCategoryRevenue = df.withColumn("rank",dense_rank over categoryBucket) 
