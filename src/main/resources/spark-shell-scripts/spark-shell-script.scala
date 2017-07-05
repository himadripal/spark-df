/**
  * Created by Himadri Pal (mehimu@gmail.com) on 7/5/2017.
  */

import java.sql.Timestamp
import java.util.regex.Pattern
import com.apple.codetest.util.Util._
import com.apple.codetest.model.ModelFactory._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import sqlContext.implicits._
val dataDir="/user/hpal/data" //point to your data dir before running

//for query 1 - sales distribution
val prod_raw = sc.textFile(dataDir+"/Product.txt")
val prod_df = prod_raw.map(x => x.split(Util.Constant.PIPE)).map(rowArr => Product(
  rowArr(Util.PRODUCT.product_id.id).toString,
  rowArr(Util.PRODUCT.product_name.id).toString,
  rowArr(Util.PRODUCT.product_price.id).toString,
  rowArr(Util.PRODUCT.product_version.id).toString,
  Util.convertToDouble(rowArr(Util.PRODUCT.product_price.id))
)
).toDF().cache();

val sales_raw = sc.textFile(dataDir+"/Sales.txt")
val sales_df = sales_raw.map(row => row.split(Util.Constant.PIPE)).map(rowArr => Sales(
  rowArr(Util.SALES.transaction_id.id),
  rowArr(Util.SALES.customer_id.id),
  rowArr(Util.SALES.product_id.id),
  Util.convertToTimestamp(rowArr(Util.SALES.timestamp.id)).get,
  Util.convertToDouble(rowArr(Util.SALES.total_amount.id)),
  rowArr(Util.SALES.total_quantity.id).toInt
)).toDF().persist(StorageLevel.MEMORY_AND_DISK);

val sales_distribution = sales_df.join(prod_df, Util.SALES.product_id.toString).groupBy(Util.PRODUCT.product_name.toString,Util.PRODUCT.product_type.toString).agg(sum(Util.SALES.total_amount.toString).alias(Util.SALES.total_amount.toString)).orderBy(desc(Util.SALES.total_amount.toString)).show(25)

//for query 2 - total sales 2013 with no refund
val refund_raw = sc.textFile(dataDir+"/Refund.txt")
val refund_df = refund_raw.map(x => x.split(Util.Constant.PIPE)).map(rowArr => Refund(
  rowArr(Util.REFUND.refund_id.id),
  rowArr(Util.REFUND.original_transaction_id.id),
  rowArr(Util.REFUND.customer_id.id),
  rowArr(Util.REFUND.product_id.id),
  Util.convertToTimestamp(rowArr(Util.REFUND.timestamp.id)).get,
  Util.convertToDouble(rowArr(Util.REFUND.refund_amount.id)),
  rowArr(Util.REFUND.refund_quantity.id).toInt
)).toDF()


val customer_raw = sc.textFile(dataDir+"/Customer.txt")
val customer_df = customer_raw.map(row => row.split(Util.Constant.PIPE)).map(rowArr => Customer(
  rowArr(Util.CUSTOMER.customer_id.id),
  rowArr(Util.CUSTOMER.customer_first_name.id),
  rowArr(Util.CUSTOMER.customer_last_name.id),
  rowArr(Util.CUSTOMER.phone_number.id)
)
).toDF();

val sales_2013 = sales_df.filter(year(sales_df(Util.SALES.timestamp.toString))===lit("2013")).persist(StorageLevel.MEMORY_AND_DISK)
val refund_2013_and_beyond = refund_df.filter(year(refund_df(Util.SALES.timestamp.toString))>=lit("2013")).select(Util.REFUND.original_transaction_id.toString,Util.REFUND.refund_amount.toString).persist(StorageLevel.MEMORY_AND_DISK);
val sales_and_refund_join = sales_2013.join(refund_2013_and_beyond,sales_2013(Util.SALES.transaction_id.toString)===refund_2013_and_beyond(Util.REFUND.original_transaction_id.toString),Util.Constant.LEFT_OUTER).persist(StorageLevel.MEMORY_AND_DISK);
val sales_no_refund_2013= sales_and_refund_join.where(refund_2013_and_beyond(Util.REFUND.original_transaction_id.toString).isNull).select(Util.SALES.total_amount.toString)
val total_sales_no_refund = sales_no_refund_2013.rdd.map(_(0).asInstanceOf[Double]).reduce(_+_)

//for query 3 - may 2013 , 2nd most purchaser
val sales_and_refund_2013_may = sales_and_refund_join.filter(month(sales_and_refund_join(Util.SALES.timestamp.toString))===lit("5")).na.fill(0.0,Seq(Util.REFUND.refund_amount.toString)).withColumn(Util.Constant.NET_SALES, $"total_amount" - $"refund_amount")
val per_customer_sale = sales_and_refund_2013_may.groupBy(Util.SALES.customer_id.toString).agg(sum(Util.Constant.NET_SALES).alias(Util.Constant.NET_SALES)).orderBy(desc(Util.Constant.NET_SALES))

val customer_id_with_2nd_most_purchase :String  = per_customer_sale.head(2).last.getAs[String](Util.SALES.customer_id.toString)

val cust_details_with_2nd_most_purchase = customer_df.select(customer_df(Util.CUSTOMER.customer_id.toString),concat_ws(Util.Constant.SPACE,
  customer_df(Util.CUSTOMER.customer_first_name.toString),
  customer_df(Util.CUSTOMER.customer_last_name.toString)
).alias(Util.Constant.CUSTOMER_NAME)
).where(Util.CUSTOMER.customer_id.toString+"="+ customer_id_with_2nd_most_purchase).show()

//for query for, no sale on any product
val prod_with_no_sale = prod_df.join(sales_df,prod_df(Util.PRODUCT.product_id.toString)===sales_df(Util.SALES.product_id.toString)).where(sales_df(Util.SALES.transaction_id.toString).isNull).show()

//query 6 - same product on the same day by same customer => count
val cust_prod_sale_day = sales_df.select(sales_df(Util.SALES.customer_id.toString),sales_df(Util.SALES.product_id.toString),date_format(sales_df(Util.SALES.timestamp.toString),"MM/dd/yyyy").alias(Util.Constant.SALE_DAY))

val total_cust_with_2_sale_a_day = cust_prod_sale_day.groupBy(Util.Constant.SALE_DAY,Util.SALES.product_id.toString,Util.SALES.customer_id.toString).agg(count("*").alias("count")).where($"count" >=2).count()

//query 7 - find a customer living on a address - case class with more than 23 attributs and upper(column) search
val customer_extend_raw = sc.textFile(dataDir+"/Customer_Extended.txt")
val customer_extended_df = customer_extend_raw.map(row => row.split(Util.Constant.PIPE)).map(rowArr => Customer_Extended(
  rowArr(Util.CUSTOMER_EXTENDED.id.id),
  rowArr(Util.CUSTOMER_EXTENDED.first_name.id),
  rowArr(Util.CUSTOMER_EXTENDED.last_name.id),
  rowArr(Util.CUSTOMER_EXTENDED.home_phone.id),
  rowArr(Util.CUSTOMER_EXTENDED.mobile_phone.id),
  rowArr(Util.CUSTOMER_EXTENDED.gender.id),
  Address(rowArr(Util.CUSTOMER_EXTENDED.current_street_address.id), rowArr(Util.CUSTOMER_EXTENDED.current_city.id), rowArr(Util.CUSTOMER_EXTENDED.current_state.id), rowArr(Util.CUSTOMER_EXTENDED.current_country.id), rowArr(Util.CUSTOMER_EXTENDED.current_zip.id)),
  Address(rowArr(Util.CUSTOMER_EXTENDED.permanent_street_address.id), rowArr(Util.CUSTOMER_EXTENDED.permanent_city.id), rowArr(Util.CUSTOMER_EXTENDED.permanent_state.id), rowArr(Util.CUSTOMER_EXTENDED.permanent_country.id), rowArr(Util.CUSTOMER_EXTENDED.permanent_zip.id)),
  Address(rowArr(Util.CUSTOMER_EXTENDED.office_street.id), rowArr(Util.CUSTOMER_EXTENDED.office_city.id), rowArr(Util.CUSTOMER_EXTENDED.office_state.id), rowArr(Util.CUSTOMER_EXTENDED.office_country.id), rowArr(Util.CUSTOMER_EXTENDED.office_zip.id)),
  rowArr(Util.CUSTOMER_EXTENDED.personal_email_address.id),
  rowArr(Util.CUSTOMER_EXTENDED.work_email_address.id),
  rowArr(Util.CUSTOMER_EXTENDED.twitter_id.id),
  rowArr(Util.CUSTOMER_EXTENDED.facebook_id.id),
  rowArr(Util.CUSTOMER_EXTENDED.linkedin_id.id)
)
).toDF()

val customer_at_addr = customer_extended_df.filter(upper($"curr_address.street_address").like("1154 Winters Blvd".toUpperCase)).show();

