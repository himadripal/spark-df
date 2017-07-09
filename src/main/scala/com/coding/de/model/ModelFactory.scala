package com.coding.de.model

import java.sql.Timestamp

/**
  * Created by Himadri Pal (mehimu@gmail.com) on 7/1/2017.
  */
object ModelFactory {

    case class Product(product_id : String, product_name : String, product_type : String, product_version : String, product_price :Double)

    case class Customer(customer_id : String, customer_first_name : String, customer_last_name : String, phone_number : String)

    case class Sales(transaction_id  : String, customer_id : String, product_id  : String, timestamp  : Timestamp, total_amount : Double, total_quantity: Int )

    case class Refund(refund_id : String, original_transaction_id : String, customer_id : String, product_id : String, timestamp : Timestamp, refund_amount :Double, refund_quantity : Int)

    case class Customer_Extended (id : String, first_name: String, last_name: String, home_phone: String, mobile_phone: String, gender: String,
                                  current_address: Address,
                                  permanent_address: Address,
                                  office_address: Address,
                                  personal_email_address: String,work_email_address: String, twitter_id: String, facebook_id: String, linkedin_id: String )
    case class Address(street_address: String, city: String, state: String, country: String, zip: String)

}
