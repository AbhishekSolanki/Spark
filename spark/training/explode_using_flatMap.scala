// "Explode!" using flatMap
// generates 1-M records from single input

// input (String)
// 1,"a,b,c"
// 2,"d,e"

// Required output in (K1,V1.1) ; (K1,V1.2) ; (K1,V1.3) ; (K2,V2.1) ; (K2,V2.2)
// (1,a)
// (1,b)
// (1,c)
// (2,d)
// (2,e)

val input = sc.parallelize(Seq(("1,\"a,b,c\""),("2,\"d,e\"")))
//input: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[5] at parallelize at <console>:32


import scala.collection.mutable.ListBuffer
val explodeRDD = rdd.flatMap( x=> {
	val rec = x.split(",\"") // key and value is splited by  ',"' delimeter
	var output = new ListBuffer[(String,String)]() // flatMap returns Traversable i.e list, sequence etc.
	val key = rec(0) // after splitting data key is the oth element
	val data = rec(1).replace("\"","").split(",") // at rec(1) value would be a,b,c" we  need to remove " and split by ,
	for( eachElementInData  <- data) output += ((key,eachElementInData)) // for each element in data we create type (key,value) and add to output
	output // for each key there are multiple output in list. Hence returning the list 
})
//explodeRDD: org.apache.spark.rdd.RDD[(String, String)] = MapPartitionsRDD[6] at flatMap at <console>:37

// let's check the output 
explodeRDD.foreach(println)

// result
// (1,a)
// (1,b)
// (1,c)
// (2,d)
// (2,e)
// (2,f)