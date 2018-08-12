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
// _3highestCuisineArr: Array[(String, Int)] = Array((Fast Food,1984), (Chinese,2733), (North Indian,3958))

val  _3highestCuisine =  _3highestCuisineArr(0) // final ans which is the third most popular cuisine
// _3highestCuisine: (String, Int) = (Fast Food,1984)


// PROBLEM 2: Which is the Fifth most "Poor" rated restaurant in each country which has online delivering option 
val zomatoRaw = sc.textFile("dataset/zomato-restaurants-data/zomato.csv").mapPartitionsWithIndex {
	(idx,itr) => if(idx==0) itr.drop(1) else itr
}.filter(line=>{
	val rec = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))")
	rec.size==21 && rec(19).trim.equals("Poor") && rec(13).trim.equals("Yes")
}) // limiting data 
// colum 9 contains , inside data like "a,b,c" and checking total columns should be 21, should be delivering "Poor" and delivering now
// only country code 1ith 1 matches this criteria

val countryPoorRestaurant = zomatoRaw.map( line => {
	val rec = line.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))")
	(rec(2),(rec(1),rec(20).toInt))
}).groupByKey() // (country,CompactBuffer[(restaurant_name,votes)])

// a method to sort inside list which will be used in map function
def fifthMostPoorRestaurant = ( input: Iterable[(String,Int)]) =>{ // takes input as compactBuffer from map
	val sortedRestaurantByVotes = input.toList.sortBy(_._2) // converting into list and sorting based on votes in ascending
	sortedRestaurantByVotes(5) // fifth element is the fith most "Poor" delivering restaurant
}

val fifthMostPoorRestaurantByCountry = countryPoorRestaurant.map( x => fifthMostPoorRestaurant(x._2)) // passing the groupByKey value to sorting function in map
// fifthMostPoorRestaurantByCountry.foreach(println)
// (Chicago Pizza,8)