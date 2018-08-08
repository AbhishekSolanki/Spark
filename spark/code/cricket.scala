// removing header from dataset
val indiaMatchDataRDD = sc.textFile("dataset/t20matches/cricket.csv").mapPartitionsWithIndex {
 (idx,itr) => if(idx==0) itr.drop(1) else itr
}.filter(x=>x.split(",")(2).equals("ind") || x.split(",")(3).equals("ind"))

val indiaTotalMatches = indiaMatchDataRDD.count.toDouble


// 1. What is India’s total Win/Loss/Tie percentage?
// (win,loss,tie)
val indiaWinLossTie = indiaMatchDataRDD.map( rec=> {
	val record =rec.split(",")
	if(record(4).equals("ind")) ("ind",(1,0,0))  // win 
	else if(record(4).equals("null")) ("ind",(0,0,1)) // tie
	else ("ind",(0,1,0)) // loose
}).reduceByKey(
(acc,n) => (acc._1+n._1,acc._2+n._2,acc._3+n._3) //totalMatches
).map( x =>{
 val totalMatches: Double = x._2._1 + x._2._2 + x._2._3
(x._2._1/(totalMatches) * 100 ,x._2._2/totalMatches *100,x._2._3/totalMatches *100)
}).first()


//2.	What is India’s Win/Loss/Tie percentage in away and home matches?
val indiaWinLossTieHA = indiaMatchDataRDD.map( rec=> {
	val record =rec.split(",")
	if(record(1).equals("ind")){
	   if(record(4).equals("ind")) ("home",(1,0,0))  // win 
	   else if(record(4).equals("null")) ("home",(0,0,1)) // tie
	   else ("home",(0,1,0)) // loose
    }else{
	   if(record(4).equals("ind")) ("away",(1,0,0))  // win 
	   else if(record(4).equals("null")) ("away",(0,0,1)) // tie
	   else ("away",(0,1,0)) // loose
	}
}).reduceByKey(
(acc,n) => (acc._1+n._1,acc._2+n._2,acc._3+n._3) //totalMatches
).map( x =>{
 val totalMatches: Double = x._2._1 + x._2._2 + x._2._3
(x._1,(x._2._1/(totalMatches) * 100 ,x._2._2/totalMatches *100,x._2._3/totalMatches *100))
}).foreach(println)

//5.	Which are the home and away grounds where India has played most number of matches?
val indiaMostPlayedVenue = indiaMatchDataRDD.map( rec=> {
 val record = rec.split(",")
 (record(1),1)
}).reduceByKey(
(acc,n) => acc+n
).sortBy(x => x._2).top(3)



//6.	What has been the average Indian win or loss by Runs per year?