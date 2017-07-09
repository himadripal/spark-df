/**
  * Created by hpal on 7/2/2017.
  */

//************************ Start loading imports and common class, functions
import java.sql.Timestamp
import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import java.text.SimpleDateFormat
import scala.util.{Failure, Success, Try}

case class Product(product_id : String, product_name : String, product_type : String, product_version : String, product_price :Double)
case class Customer(customer_id : String, customer_first_name : String, customer_last_name : String, phone_number : String)
case class Sales(transaction_id  : String, customer_id : String, product_id  : String, timestamp  : Timestamp, total_amount : Double, total_quantity: Int )
case class Refund(refund_id : String, original_transaction_id : String, customer_id : String, product_id : String, timestamp : Timestamp, refund_amount :Double, refund_quantity : Int)
case class Address (street_address: String, current_city: String, current_state: String, current_country: String, current_zip: String)
case class Customer_Extended (id : String, first_name: String, last_name: String, home_phone: String, mobile_phone: String, gender: String, curr_address : Address, permanent_address : Address, office_address: Address, personal_email_address: String, work_email_address: String, twitter_id: String, facebook_id: String, linkedin_id: String )


def convertToDouble(input :String ):Double = input match {
  case "" => 0.0
  case _ => {
    var output = "";
    Try {
      for (s <- input.toCharArray) {
        if (((s - '0') >= 0 && (s - '0') <= 9) || s == '.') output += s;
      }
      output.toDouble
    }match {
      case Success(value) => value
      case Failure(_) => 0.0
    }
  }
}
def convertToTimestamp(input: String) : Option[Timestamp] = input match {
  case "" => None
  case _ => {
    val format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
    Try(new Timestamp(format.parse(input).getTime)) match {
      case Success(time) => Some(time)
      case Failure(_) => None
    }
  }
}
//end loading common functions

// ************
      // IMPORTATNT - CHANGE THE PATH to POINT TO YOUR HDFS data dir.
//*************
val dataDir = "/user/svcinvpii/callcenter_test/data";


//************************************** Start Load DF*********************************************************************
import spark.implicits._
val prod_raw = spark.sparkContext.textFile(dataDir+"/Product.txt")
val prod_df = prod_raw.map(x => x.split("\\|")).map(rowArr => Product(
  rowArr(0),
  rowArr(1),
  rowArr(2),
  rowArr(3),
  convertToDouble(rowArr(4)))
).toDF().cache();

val sales_raw = spark.sparkContext.textFile(dataDir+"/Sales.txt")
val sales_df = sales_raw.map(row => row.split("\\|")).map(rowArr => Sales(
  rowArr(0),
  rowArr(1),
  rowArr(2),
  convertToTimestamp(rowArr(3)).get,
  convertToDouble(rowArr(4)),
  rowArr(5).toInt
)).toDF().persist(StorageLevel.MEMORY_AND_DISK);

val refund_raw = spark.sparkContext.textFile(dataDir+"/Refund.txt")
val refund_df = refund_raw.map(x => x.split("\\|")).map(rowArr => Refund(
  rowArr(0),
  rowArr(1),
  rowArr(2),
  rowArr(3),
  convertToTimestamp(rowArr(4)).get,
  convertToDouble(rowArr(5)),
  rowArr(6).toInt
)).toDF()
val customer_raw = spark.sparkContext.textFile(dataDir+"/Customer.txt")
val customer_df = customer_raw.map(row => row.split("\\|")).map(rowArr => Customer(
  rowArr(0),
  rowArr(1),
  rowArr(2),
  rowArr(3)
)
).toDF();

val customer_extend_raw = spark.sparkContext.textFile(dataDir+"/Customer_Extended.txt")
val customer_extended_df = customer_extend_raw.map(row => row.split("\\|")).map(rowArr => Customer_Extended(
  rowArr(0),
  rowArr(1),
  rowArr(2),
  rowArr(3),
  rowArr(4),
  rowArr(5),
  Address(rowArr(6), rowArr(7), rowArr(8), rowArr(9), rowArr(10)),
  Address(rowArr(11),rowArr(12),rowArr(13),rowArr(14),rowArr(15)),
  Address(rowArr(16),rowArr(17),rowArr(18),rowArr(19),rowArr(20)),
  rowArr(21),
  rowArr(22),
  rowArr(23),
  rowArr(24),
  rowArr(25))
).toDF()

println("loaded all dfs")
//End load all dfs

//********************************** Start Query ***************************************
// copy past from here "2. distribution of sales by product name and product type.")
println("2. distribution of sales by product name and product type.=============")
val sales_distribution = sales_df.join(prod_df, "product_id").
  groupBy("product_name","product_type").agg(sum("total_amount").alias("total_amount")).orderBy(desc("total_amount")).show(25)

// query 2 ends here.

//****************************************************************************************************************************
// copy paste from here "3. Total amount of all transactions that happened in year 2013 and have not been refunded as of today.")
println("3. Total amount of all transactions that happened in year 2013 and have not been refunded as of today =============")

val sales_2013 = sales_df.filter(year(sales_df("timestamp"))===lit("2013")).persist(StorageLevel.MEMORY_AND_DISK)
//refund should be in 2013 or beyond, caching for next use
val refund_2013_and_beyond = refund_df.filter(year(refund_df("timestamp"))>=lit("2013")).
  select("original_transaction_id", "refund_amount").persist(StorageLevel.MEMORY_AND_DISK);

//left outer join with refund and cache it
val sales_and_refund_join = sales_2013.join(refund_2013_and_beyond,
  sales_2013("transaction_id")===refund_2013_and_beyond("original_transaction_id"),"left_outer").persist(StorageLevel.MEMORY_AND_DISK);

//as left outer join was used, all matching rows from sales_df will come with all columns for refund_df as null
// hence all transactions ids which does not have a refund will have original_transaction_id as null,
// hence filtering all rows where ORIGINAL_TRANSACTION_ID is null
val sales_no_refund_2013= sales_and_refund_join.where(refund_2013_and_beyond("original_transaction_id").isNull).select("total_amount")
//its efficient to use internal rdd of the data frame for map and reduce
//need total of all rows from previous dataframe
val total_sales_no_refund = sales_no_refund_2013.rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)

// end query 3"===============================================================================================================================")

//***************************************************************************************************************************
//copy paste from here "4. customer name who made the second most purchases in the month of May 2013. Refunds be excluded")
//from sales_and_refund cached dataframe filter the records for the month of may
//fill all null refund_amount column with 0.0
//derive a columne net_sales = sales_df(total_amount) - refund_df(refund_amount)
println("4. customer name who made the second most purchases in the month of May 2013. Refunds be excluded==================")
val sales_and_refund_2013_may = sales_and_refund_join.filter(month(sales_and_refund_join("timestamp"))===lit("5")).
  na.fill(0.0,Seq("refund_amount")).withColumn("net_sales", $"total_amount" - $"refund_amount")

//group by customer_id and aggregate  using sum
// total_sales sorted on desc order, most purchase at the top
val per_customer_sale = sales_and_refund_2013_may.groupBy("customer_id").agg(sum("net_sales").alias("net_sales")).orderBy(desc("net_sales"))
//head(2) will take first two and last (2nd row) will be the second most
// extract customer_id from the last (2nd row)
val customer_id_with_2nd_most_purchase :String  = per_customer_sale.head(2).last.getAs[String]("customer_id")


//retrive first_name ,last_name concat with space for the customer_id calculated above
//using show for nice tabular format and its only 1 row
val cust_details_with_2nd_most_purchase = customer_df.select(customer_df("customer_id"),concat_ws(" ", customer_df("customer_first_name"), customer_df("customer_last_name")).
  alias("customer_name")).where("customer_id"+"="+ customer_id_with_2nd_most_purchase).show()

// query 4 ends here "===============================================================================================================================")

//*********************************************************************************************************
//copy paste from here for query 5. product that has not been sold at least once (if any).")
//join product with sales df with inner join
println("5. product that has not been sold at least once (if any).=================")
val prod_with_no_sale = prod_df.join(sales_df,
  prod_df("product_id")===sales_df("product_id"),"left_outer").where(sales_df("transaction_id").isNull).show()

//query 5 ends

//**************************************************************************************************
//copy paste from here query 6 . total number of users who purchased the same product consecutively at least 2 times on a given day."
println("6 . total number of users who purchased the same product consecutively at least 2 times on a given day.============")
val cust_prod_sale_day = sales_df.select(sales_df("customer_id"),
  sales_df("product_id"),
  date_format(sales_df("timestamp"),"MM/dd/yyyy").alias("sale_day"))

val total_cust_with_2_sale_a_day = cust_prod_sale_day.groupBy("sale_day",
  "product_id",
  "customer_id").agg(count("*").alias("count")).where($"count" >=2).count()



//query 6 ends here "total customers - "+total_cust_with_2_sale_a_day)

//****************************************************************************************************
//copyquery 7. additional - all the details of a customer who is currently living at 1154 Winters Blvd.")
println("7. additional - all the details of a customer who is currently living at 1154 Winters Blvd.============")

val customer_at_addr = customer_extended_df.filter(upper($"curr_address.street_address").like("1154 Winters Blvd".toUpperCase)).show();
//query 7 end here ("===============================================================================================================================")
