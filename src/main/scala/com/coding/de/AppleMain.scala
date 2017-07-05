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
object AppleMain extends  Util{

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
      ).toDF().persist(StorageLevel.MEMORY_AND_DISK);
    /*Persisting as it will be used later, intentional persist instead of cache to specify spark to use both memory and disk */


    //join product and sales data and find out sales distribution by product name and product type
    //using show to get the nice tabular format,  show by default shows 20 rows, passing show(100) to show all combinations in this case
    // output can be saved if the requirement be.
    println("==============================================================================================================================")
    println("2. distribution of sales by product name and product type.")
    val sales_distribution = sales_df.join(prod_df, SALES.PRODUCT_ID.toString)
                                        .groupBy(PRODUCT.PRODUCT_NAME.toString,PRODUCT.PRODUCT_TYPE.toString)
                                        .agg(sum(SALES.TOTAL_AMOUNT.toString).alias(SALES.TOTAL_AMOUNT.toString)).orderBy(desc(SALES.TOTAL_AMOUNT.toString)).show(25)

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


    val customer_raw = spark.sparkContext.textFile(dataDir+"/Customer.txt")
    val customer_df = customer_raw.map(row => row.split(Pattern.quote(Constant.PIPE))).map(rowArr => Customer(
                                                                              rowArr(CUSTOMER.CUSTOMER_ID.id),
                                                                              rowArr(CUSTOMER.CUSTOMER_FIRST_NAME.id),
                                                                              rowArr(CUSTOMER.CUSTOMER_LAST_NAME.id),
                                                                              rowArr(CUSTOMER.PHONE_NUMBER.id)
                                                                                )
                                                              ).toDF();

    println("===============================================================================================================================")
    println("3. Total amount of all transactions that happened in year 2013 and have not been refunded as of today.")

    //to work with only 2013 the following two queries
    //persisting/caching both sales and refund as these two will be used for the next query
    val sales_2013 = sales_df.filter(year(sales_df(SALES.TIMESTAMP.toString))===lit("2013")).persist(StorageLevel.MEMORY_AND_DISK)
    //refund should be in 2013 or beyond, caching for next use
    val refund_2013_and_beyond = refund_df.filter(year(refund_df(SALES.TIMESTAMP.toString))>=lit("2013")).
                                          select(REFUND.ORIGINAL_TRANSACTION_ID.toString,
                                                  REFUND.REFUND_AMOUNT.toString)
                                          .persist(StorageLevel.MEMORY_AND_DISK);

    //left outer join with refund and cache it
    val sales_and_refund_join = sales_2013.join(refund_2013_and_beyond,
                                                sales_2013(SALES.TRANSACTION_ID.toString)===refund_2013_and_beyond(REFUND.ORIGINAL_TRANSACTION_ID.toString),
                                                Constant.LEFT_OUTER).persist(StorageLevel.MEMORY_AND_DISK);

    //as left outer join was used, all matching rows from sales_df will come with all columns for refund_df as null
    // hence all transactions ids which does not have a refund will have original_transaction_id as null,
    // hence filtering all rows where ORIGINAL_TRANSACTION_ID is null
    val sales_no_refund_2013= sales_and_refund_join.where(refund_2013_and_beyond(REFUND.ORIGINAL_TRANSACTION_ID.toString).isNull).select(SALES.TOTAL_AMOUNT.toString)
    //its efficient to use internal rdd of the data frame for map and reduce
    //need total of all rows from previous dataframe
    val total_sales_no_refund = sales_no_refund_2013.rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)

    println("total = "+total_sales_no_refund)
    println("===============================================================================================================================")


    println("===============================================================================================================================")
    println("4. customer name who made the second most purchases in the month of May 2013. Refunds be excluded")
    //from sales_and_refund cached dataframe filter the records for the month of may
    //fill all null refund_amount column with 0.0
    //derive a column net_sales = sales_df(total_amount) - refund_df(refund_amount)
    val sales_and_refund_2013_may = sales_and_refund_join.filter(month(sales_and_refund_join(SALES.TIMESTAMP.toString))===lit("5"))
                                  .na.fill(0.0,Seq(REFUND.REFUND_AMOUNT.toString))
                                  .withColumn(Constant.NET_SALES, $"total_amount" - $"refund_amount") //could have used names from ENUM

    //group by customer_id and aggregate  using sum
    // total_sales sorted on desc order, most purchase at the top
    val per_customer_sale = sales_and_refund_2013_may.groupBy(SALES.CUSTOMER_ID.toString)
                                                    .agg(sum(Constant.NET_SALES).alias(Constant.NET_SALES))
                                                    .orderBy(desc(Constant.NET_SALES))


    //head(2) will take first two and last (2nd row) will be the second most
    // extract customer_id from the last (2nd row)
    val customer_id_with_2nd_most_purchase :String  = per_customer_sale.head(2).last.getAs[String](SALES.CUSTOMER_ID.toString)

    //retrive first_name ,last_name concat with space for the customer_id calculated above
    //using show for nice tabular format and its only 1 row
    val cust_details_with_2nd_most_purchase = customer_df.select(concat_ws(Constant.SPACE,
                                                                          customer_df(CUSTOMER.CUSTOMER_FIRST_NAME.toString),
                                                                          customer_df(CUSTOMER.CUSTOMER_LAST_NAME.toString)
                                                                          ).alias(Constant.CUSTOMER_NAME)
                                                                ).where(CUSTOMER.CUSTOMER_ID.toString+"="+ customer_id_with_2nd_most_purchase).show()

    println("===============================================================================================================================")


    println("================================================================================================================================")
    println("5. product that has not been sold at least once (if any).")
    //join product with sales df with inner join
    //left outer join prod_df with sales_df will associate all transactions for each product id, if there is a product which does not have record in sales
    // then transaction id will be null, those are the products not sold at least once.
    val prod_with_no_sale = prod_df.join(sales_df,
                                        prod_df(PRODUCT.PRODUCT_ID.toString)===sales_df(SALES.PRODUCT_ID.toString),Constant.LEFT_OUTER).
                                        where(sales_df(SALES.TRANSACTION_ID.toString).isNull).show()

    println("===============================================================================================================================")


    println("===============================================================================================================================")
    println(" 6 . total number of users who purchased the same product consecutively at least 2 times on a given day.")
    // format the date without time, seect customer id and product id as well
    val cust_prod_sale_day = sales_df.select(sales_df(SALES.CUSTOMER_ID.toString),
                                            sales_df(SALES.PRODUCT_ID.toString),
                                            date_format(sales_df(SALES.TIMESTAMP.toString),"MM/dd/yyyy").alias(Constant.SALE_DAY))

    //group by sale_day,product_id and customer_id having count of those records >=2  meaning at least twice
    // Assumption : refind has not been taken into account , meaning if a person bought and then refunded on the same day, it will still count as one purchase
    // on that day
     val total_cust_with_2_sale_a_day = cust_prod_sale_day.groupBy(Constant.SALE_DAY,
                                                                  SALES.PRODUCT_ID.toString,
                                                                  SALES.CUSTOMER_ID.toString)
                                                                  .agg(count("*").alias("count"))
                                                                  .where($"count" >=2).count()

    println("   total customers - "+total_cust_with_2_sale_a_day)
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
val customer_at_addr = customer_extended_df.filter(upper($"current_address.street_address").like("1154 Winters Blvd".toUpperCase)).show();
println("===============================================================================================================================")
}

}
