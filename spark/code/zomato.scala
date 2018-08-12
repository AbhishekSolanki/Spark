// Dataset : https://github.com/AbhishekSolanki/Spark/tree/master/dataset/zomato-restaurants-data/zomato.csv
// Data Dictionary: https://github.com/AbhishekSolanki/Spark/tree/master/dataset/zomato-restaurants-data/zomato_data_dictonary.txt

// PROBLEM 1: Which is the third most popular cuisine served by restaurant
val zomatoRaw = sc.textFile("dataset/zomato-restaurants-data/zomato.csv").mapPartitionsWithIndex {
	(idx,itr) => if(idx==0) itr.drop(1) else itr
}.filter(line=>line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))").size==21) // colum 9 contains , inside data like "a,b,c" and checking total columns should be 21

import scala.collection.mutable.ListBuffer
val _3highestCuisineArr = zomatoRaw.flatMap( line => {
	val restaurantCuisines = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))")(9) // splitting libe by comma with "," inside data and getting the 9th column
	val restaurantCuisinesArray = restaurantCuisines.replace("\"","").split(",") // each cussines served by restaurant are seperated by ","
	val output = new ListBuffer[(String,Int)]()
	for( cuisine <- restaurantCuisinesArray ) output+=((cuisine.trim,1)) // creating tuple (cusine,1)
	 output
}).reduceByKey(_+_). // (cuisine_type,restaurant_serving_cuisine_count)
takeOrdered(3)(Ordering[Int].reverse.on(_._2)). // getting top 3 cuisine in descending order
sortBy(_._2) // ascending sorting hence 3rd higest cuisine will be on top

val  _3highestCuisine =  _3highestCuisineArr(0) // final ans which is the third most popular cuisine

