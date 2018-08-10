//Aggregation practice
// dataset: retail_db

// 1. total revenue per order from order_items dataset

val order_items = sc.textFile("dataset/retail_db/order_items/part-00000")
val revenuePerOrder = order_items.map(x=>(x.split(",")(1).toInt,x.split(",")(4).toFloat)).
reduceByKey((total,value)=>total+value)

// 1.1 top 3 orders based on revenue
val top3OrderByRevenue = order_items.map(x=>(x.split(",")(1).toInt,x.split(",")(4).toFloat)).
reduceByKey((total,value)=>total+value).sortBy(a => -a._2)

top3OrderByRevenue.top(3)(Ordering[Float].on(x=>x._2))
//OR 
top3OrderByRevenue.take(3)


// 2. Revenue and # of items sold in each order_items

val order_items = sc.textFile("dataset/retail_db/order_items/part-00000")
val revenueAndItemsPerOrder = order_items.map(x=>(x.split(",")(1).toInt,x.split(",")(4).toFloat)).
aggregateByKey((0.0,0))(
(cbr,ele) => (cbr._1+ele,cbr._2+1) ,
(cbr,red) => (cbr._1+red._1, cbr._2+red._2)
)
// input = (K:String,V:Float)
// output = (K:String,(V1:Float, V2:Int)) 
// aggregateByKey(initialize)(
// combiner(cbr,ele),
// reducer(cbr,red)
// )

// initailize type is the output type required
// ele type is same as input type
// cbr and red type are same as output type

// using reduceByKey 
val revenueAndItemsPerOrder = order_items.map(x=>(x.split(",")(1).toInt,(x.split(",")(4).toFloat,1))).
reduceByKey( (total,value) => (total._1+value._1,total._2+value._2))


// 2.1 Revenue and # of items sold in each order_items top 10 by revenue 
val revenueAndItemsPerOrderSortedByRevenue = revenueAndItemsPerOrder.sortBy(a => -a._2._1).take(10)

// 2.2 Revenue and # of items sold in each order_items top 10 by items 
val revenueAndItemsPerOrderSortedByRevenue = revenueAndItemsPerOrder.sortBy(a => -a._2._2).take(10)

// 2.2 Revenue and # of items sold in each order_items top 10 by revenue and items 
val revenueAndItemsPerOrderSortedByRevenue = revenueAndItemsPerOrder.sortBy(a => (-a._2._1,-a._2._2)).take(10)


// 3. compute revenue,Min , Max and Average price of items per order 
// approach 1 for calulating average with map map function after reducer
val order_items = sc.textFile("dataset/retail_db/order_items/part-00000")
val revenuePerOrder = order_items.map(x=>(x.split(",")(1).toInt,(x.split(",")(4).toFloat,
	x.split(",")(4).toFloat,x.split(",")(4).toFloat,1.toFloat))).
reduceByKey(
(a,b) => {
// min 
var sum:Float = a._1+b._1
var min:Float = 0
var max:Float  =0
var items = (a._4+b._4)
if(a._2< b._2) min=a._2 else min=b._2
if(a._3 > b._3) max =a._3 else max=b._3
(sum,min,max,items)
}).map(x=>(x._1,(x._2._1,x._2._2,x._2._3,x._2._1/x._2._4))).filter(_._1==4).foreach(println)


// approach 2 for calulating average with extra value in map
val revenuePerOrder = order_items.map(x=>(x.split(",")(1).toInt,(x.split(",")(4).toFloat,
	x.split(",")(4).toFloat,x.split(",")(4).toFloat,0.toFloat,1.toFloat))).
reduceByKey(
(a,b) => {
var sum:Float = a._1+b._1
var min:Float = 0
var max:Float  =0
var items = a._5+b._5
var avg = sum /items
if(a._2< b._2) min=a._2 else min=b._2
if(a._3 > b._3) max =a._3 else max=b._3
(sum,min,max,avg,items)
})


// implementing groupByKey using combineByKey

val products = sc.textFile("/user/cloudera/dataset/retail_db/products/")
val productsMap = products.
filter( product => product.split(",")(4) !="").
map(product => (product.split(",")(1).toInt, product)) // categoryid, product
// map output => (KEY,String),(KEY,String2)
// combine op => (KEY, LIST(String1,String2))

// combineByKey implementation

// first function that accepts current value as parameter and return new value that will
// be merged with additional values
val createCombiner = ( value: String) => List(value)

// second is the merging function, that takes a value and merges into previously collected values
// at partition level
val createMerger = ( collector1:List[String], value:String) => {
	value :: collector1
}

// third function combines the merged values together. Takes the values produced at partition level
// and combines them untill we end up at singular value.
val finalMerger = ( collector1:List[String], collector2:List[String]) =>{ 
collector1 ::: collector2
}

//validating the output of groupByKey and combineByKey
productsMap.combineByKey(createCombiner,createMerger,finalMerger).filter(_._1==13).map(x=>(x._1,x._2.size))
productsMap.groupByKey().filter(_._1==13).map(x=>(x._1,x._2.size))