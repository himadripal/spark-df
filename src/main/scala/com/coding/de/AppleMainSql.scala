package com.coding.de

import java.util.regex.Pattern

import com.coding.de.model.ModelFactory._
import com.coding.de.util.Util
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
  * Created by Himadri Pal (mehimu@gmail.com)
  * Extend Util trait to have all the utility methods and constants and enums available to this object
  */
object AppleMainSql extends  Util{

  def main(args : Array[String]) = {

    val dataDir = args(0)

    val spark = SparkSession.builder().appName("Apple Coding Test").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR");
    //import implicits for RDD to DataFrame conversion .toDF()
    import spark.implicits._
    //load Product.txt file in prodRaw rdd
    val prod_raw = spark.sparkContext.textFile(dataDir+"/Product.txt")

    //convert prodRaw rdd to a data frame with Product case class attribute names as column names
    val prod_df = prod_raw.map(x => x.split(Pattern.quote(Constant.PIPE))).map(rowArr => Product(
                                                                                rowArr(PRODUCT.PRODUCT_ID.id),
                                                                                rowArr(PRODUCT.PRODUCT_NAME.id),
                                                                                rowArr(PRODUCT.PRODUCT_TYPE.id),
                                                                                rowArr(PRODUCT.PRODUCT_VERSION.id),
                    /* convert all locale formatted string to double in df */   convertToDouble(rowArr(PRODUCT.PRODUCT_PRICE.id))
                                                                )
                                                      ).toDF().cache();
      prod_df.createOrReplaceTempView("product")
    //load Sales.txt file in salesRaw rdd
    val sales_raw = spark.sparkContext.textFile(dataDir+"/Sales.txt")
    //convert salesRaw to a data frame using Sales case class attirbute names as column names.
    val sales_df = sales_raw.map(row => row.split(Pattern.quote(Constant.PIPE))).map(rowArr => Sales(
                                                                                        rowArr(SALES.TRANSACTION_ID.id),
                                                                                        rowArr(SALES.CUSTOMER_ID.id),
                                                                                        rowArr(SALES.PRODUCT_ID.id),
                               /*convert each string to timestamp on df*/               convertToTimestamp(rowArr(SALES.TIMESTAMP.id)).get,
                               /*convert each locale formated string to double*/        convertToDouble(rowArr(SALES.TOTAL_AMOUNT.id)),
                                                                                        rowArr(SALES.TOTAL_QUANTITY.id).toInt
                                                                                        )
      ).toDF() //.persist(StorageLevel.MEMORY_AND_DISK);

    sales_df.createOrReplaceTempView("sales")
    /*Persisting as it will be used later, intentional persist instead of cache to specify spark to use both memory and disk */

    //join product and sales data and find out sales distribution by product name and product type
    //using show to get the nice tabular format,  show by default shows 20 rows, passing show(100) to show all combinations in this case
    // output can be saved if the requirement be.
    println("==============================================================================================================================")
    println("2. distribution of sales by product name and product type.")
    /*val sales_distribution = sales_df.join(prod_df, SALES.PRODUCT_ID.toString)
                                        .groupBy(PRODUCT.PRODUCT_NAME.toString,PRODUCT.PRODUCT_TYPE.toString)
                                        .agg(sum(SALES.TOTAL_AMOUNT.toString).alias(SALES.TOTAL_AMOUNT.toString)).orderBy(desc(SALES.TOTAL_AMOUNT.toString)).show(25)*/

    val sales_distribution = spark.sql("select p.product_name,p.product_type,sum(s.total_amount) as total_sales from product p left outer join " +
      "sales s on s.product_id=p.product_id group by p.product_name,p.product_type order by total_sales desc").show(30)
    //orderby is not in the question but added it to give more meaning the dat
    // 25 -> as there are only those many product name and type combinations.
    println("==============================================================================================================================")
      //load Refund.txt in refund_raw
    // create the sales table from sales text file
    val refund_raw = spark.sparkContext.textFile(dataDir+"/Refund.txt")
    val refund_df = refund_raw.map(x => x.split(Pattern.quote(Constant.PIPE))).map(rowArr => Refund(
                                                                      rowArr(REFUND.REFUND_ID.id),
                                                                      rowArr(REFUND.ORIGINAL_TRANSACTION_ID.id),
                                                                      rowArr(REFUND.CUSTOMER_ID.id),
                                                                      rowArr(REFUND.PRODUCT_ID.id),
                                                                      convertToTimestamp(rowArr(REFUND.TIMESTAMP.id)).get,
                                                                      convertToDouble(rowArr(REFUND.REFUND_AMOUNT.id)),
                                                                      rowArr(REFUND.REFUND_QUANTITY.id).toInt
                                                                      )
    ).toDF()
    refund_df.createOrReplaceTempView("refund")

    val customer_raw = spark.sparkContext.textFile(dataDir+"/Customer.txt")
    val customer_df = customer_raw.map(row => row.split(Pattern.quote(Constant.PIPE))).map(rowArr => Customer(
                                                                              rowArr(CUSTOMER.CUSTOMER_ID.id),
                                                                              rowArr(CUSTOMER.CUSTOMER_FIRST_NAME.id),
                                                                              rowArr(CUSTOMER.CUSTOMER_LAST_NAME.id),
                                                                              rowArr(CUSTOMER.PHONE_NUMBER.id)
                                                                                )
                                                              ).toDF();
    customer_df.createOrReplaceTempView("customer")
    println("===============================================================================================================================")
    println("3. Total amount of all transactions that happened in year 2013 and have not been refunded as of today.")

    val sales_wt_no_rfnd = spark.sql("select sum(a.total) as total_sales_with_no_refund from (select s.total_amount as total from sales s left outer join refund r on " +
      "s.transaction_id=r.original_transaction_id where year(s.timestamp)==2013 and r.original_transaction_id is null) a").show()


    println("===============================================================================================================================")


    println("===============================================================================================================================")
    println("4. customer name who made the second most purchases in the month of May 2013. Refunds be excluded")
    val cust_with_2nd_most_purchase = spark.sql("select concat_ws(' ',customer_first_name,customer_last_name) as name,net_sales,dense_rank from " +
      "(select sum(s.total_amount - coalesce(r.refund_amount,0L)) as net_sales,s.customer_id as customer_id," +
      "dense_rank() over(order by sum(s.total_amount - coalesce(r.refund_amount,0L)) desc) as dense_rank " +
      "from sales s left outer join refund r on s.transaction_id=r.original_transaction_id where year(s.timestamp)=2013 and month(s.timestamp)=5 group by s.customer_id) a " +
      "inner join customer c on a.customer_id=c.customer_id where dense_rank=2").show()



    println("================================================================================================================================")
    println("5. product that has not been sold at least once (if any).")

    val prod_with_no_sale = spark.sql("select p.* from product p left outer join sales s on p.product_id=s.product_id where s.transaction_id is null")

    println("===============================================================================================================================")


    println("===============================================================================================================================")
    println(" 6 . total number of users who purchased the same product consecutively at least 2 times on a given day.")
    // format the date without time, seect customer id and product id as well
    //group by sale_day,product_id and customer_id having count of those records >=2  meaning at least twice
    // Assumption : refind has not been taken into account , meaning if a person bought and then refunded on the same day, it will still count as one purchase
    // on that day
    val total_cust_with_2_sale_a_day = spark.sql("select count(*) as total_cust_with_sales_twice_a_day from (select count(*) as count,date_format(timestamp, 'MM/dd/yyyy') as day,product_id,customer_id from sales " +
    "group by date_format(timestamp, 'MM/dd/yyyy'),product_id,customer_id having count >=2) a").show()

    println("==============================================================================================================================")


    println("===============================================================================================================================")
    println(" 7. additional - all the details of a customer who is currently living at 1154 Winters Blvd.")
    val customer_extend_raw = spark.sparkContext.textFile(dataDir+"/Customer_Extended.txt")
    //22 was max number of attributes a case class could have, with 2.11 that restriction was removed for case class but still present with functions and tuples
    //here a nested case class has been used to tackle that issue, also its logical to have one Address type for 3 different addresses.(office,current and permanent)
    val customer_extended_df = customer_extend_raw.map(row => row.split(Pattern.quote(Constant.PIPE))).map(rowArr => Customer_Extended(
                                                                  rowArr(CUSTOMER_EXTENDED.ID.id),
                                                                  rowArr(CUSTOMER_EXTENDED.FIRST_NAME.id),
                                                                  rowArr(CUSTOMER_EXTENDED.LAST_NAME.id),
                                                                  rowArr(CUSTOMER_EXTENDED.HOME_PHONE.id),
                                                                  rowArr(CUSTOMER_EXTENDED.MOBILE_PHONE.id),
                                                                  rowArr(CUSTOMER_EXTENDED.GENDER.id),
      /*Address type for current address*/                        Address(rowArr(CUSTOMER_EXTENDED.CURRENT_STREET_ADDRESS.id),
                                                                          rowArr(CUSTOMER_EXTENDED.CURRENT_CITY.id),
                                                                          rowArr(CUSTOMER_EXTENDED.CURRENT_STATE.id),
                                                                          rowArr(CUSTOMER_EXTENDED.CURRENT_COUNTRY.id),
                                                                          rowArr(CUSTOMER_EXTENDED.CURRENT_ZIP.id)),
      /*Address type for permanent address*/                      Address(rowArr(CUSTOMER_EXTENDED.PERMANENT_STREET_ADDRESS.id),
                                                                         rowArr(CUSTOMER_EXTENDED.PERMANENT_CITY.id),
                                                                         rowArr(CUSTOMER_EXTENDED.PERMANENT_STATE.id),
                                                                         rowArr(CUSTOMER_EXTENDED.PERMANENT_COUNTRY.id),
                                                                         rowArr(CUSTOMER_EXTENDED.PERMANENT_ZIP.id)),
      /*Address type for permanent address*/                     Address(rowArr(CUSTOMER_EXTENDED.OFFICE_STREET.id),
                                                                         rowArr(CUSTOMER_EXTENDED.OFFICE_CITY.id),
                                                                         rowArr(CUSTOMER_EXTENDED.OFFICE_STATE.id),
                                                                         rowArr(CUSTOMER_EXTENDED.OFFICE_COUNTRY.id),
                                                                         rowArr(CUSTOMER_EXTENDED.OFFICE_ZIP.id)),
                                                                 rowArr(CUSTOMER_EXTENDED.PERSONAL_EMAIL_ADDRESS.id),
                                                                 rowArr(CUSTOMER_EXTENDED.WORK_EMAIL_ADDRESS.id),
                                                                 rowArr(CUSTOMER_EXTENDED.TWITTER_ID.id),
                                                                 rowArr(CUSTOMER_EXTENDED.FACEBOOK_ID.id),
                                                                 rowArr(CUSTOMER_EXTENDED.LINKEDIN_ID.id)
                                                                )
                                                       ).toDF()
    //we need to search only on current_address also both column and value is switch cased to UPPER. used like but = would also work for this case
 customer_extended_df.createOrReplaceTempView("customer_extnd")
 val cust_at_addr=spark.sql("select * from customer_extnd where upper(current_address.street_address)=upper('1154 Winters Blvd')").show()
println("===============================================================================================================================")
}

}
