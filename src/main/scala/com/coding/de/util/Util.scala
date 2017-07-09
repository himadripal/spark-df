package com.coding.de.util

import java.text.SimpleDateFormat
import java.sql.Timestamp

import scala.util.{Failure, Success, Try}

/**
  * Created by Himadri Pal (mehimu@gmail.com).
  */
trait Util {
    object Constant {
      val PIPE = "|"
      val LEFT_OUTER = "left_outer"
      val NET_SALES = "net_total"
      val CUSTOMER_NAME = "customer_name"
      val SPACE = " ";
      val SALE_DAY= "sale_day"
    }

    //sequence is important here, this the sequence data appears in the text file
    object PRODUCT extends Enumeration {val PRODUCT_ID,PRODUCT_NAME,PRODUCT_TYPE,PRODUCT_VERSION,PRODUCT_PRICE = Value}
    object SALES extends Enumeration   {val TRANSACTION_ID, CUSTOMER_ID, PRODUCT_ID, TIMESTAMP, TOTAL_AMOUNT, TOTAL_QUANTITY = Value}
    object REFUND extends Enumeration {val REFUND_ID, ORIGINAL_TRANSACTION_ID, CUSTOMER_ID, PRODUCT_ID, TIMESTAMP, REFUND_AMOUNT,REFUND_QUANTITY = Value}
    object CUSTOMER extends Enumeration {val CUSTOMER_ID, CUSTOMER_FIRST_NAME, CUSTOMER_LAST_NAME, PHONE_NUMBER = Value}
    object CUSTOMER_EXTENDED extends Enumeration { val ID, FIRST_NAME, LAST_NAME, HOME_PHONE, MOBILE_PHONE, GENDER, CURRENT_STREET_ADDRESS, CURRENT_CITY, CURRENT_STATE,
                                      CURRENT_COUNTRY, CURRENT_ZIP, PERMANENT_STREET_ADDRESS, PERMANENT_CITY, PERMANENT_STATE, PERMANENT_COUNTRY, PERMANENT_ZIP,
                                      OFFICE_STREET, OFFICE_CITY, OFFICE_STATE, OFFICE_COUNTRY, OFFICE_ZIP, PERSONAL_EMAIL_ADDRESS,
                                      WORK_EMAIL_ADDRESS, TWITTER_ID, FACEBOOK_ID, LINKEDIN_ID= Value}

  /**
    * Convert to double, ignore all special chars like $ nad ,
    * if it is not proper double value, function will return 0.0
    * @param input
    * @return
    */
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

  /**
    * Convert to timestamp
    * @param input
    * @return
    */
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


}
