Candidate - Himadri Pal
Data Engineer - Coding Test

======================================================================================================================================================
Data Quality Issue -

1. Write down the data quality issues with the datasets provided and steps performed to clean (if any)
-----------------+--------------------------------------------------------------+--------------------------------------------------------------------
    TABLE        |     DATA QUALITY ISSUE                                       |      STEPS TO CLEAN
-----------------+--------------------------------------------------------------+--------------------------------------------------------------------
    Product      |      No DQ Issue                                             |
-----------------+--------------------------------------------------------------+--------------------------------------------------------------------
    Customer     |      customer_id=81530 has no first_name                     |   No measures required as null may be a legitimate value
-----------------+--------------------------------------------------------------+--------------------------------------------------------------------
    Sales        |      total_amount is not uniformly formatted                 |   calling Util.convertToDouble for each total_amount while
                 |      in US locale, i.e - 4995 - no $ and ,                   |   loading into dataframe from text file. In that method,
                 |                                                              |   considering only numeric chars and calling _.toDouble
-----------------+--------------------------------------------------------------+--------------------------------------------------------------------
    Refund       |      1. total_amount is not uniformly formatted              |   1. calling Util.convertToDouble for each total_amount while
                 |      in US locale, i.e - 1449( missing '$' and ',')          |     loading into dataframe from text file. In that method,
                 |                                                              |     considering only numeric chars and calling _.toDouble
                 |      2. No value for refund_amount and refund_quantity       |   2. No Value(null or empty) is replaced with 0.0 in Util.
                 |       in the last row                                        |      convertToDouble
-----------------+--------------------------------------------------------------+---------------------------------------------------------------------
    Customer     |      1. current_address and permanent_street_address contain |   1. Make all upper case while loading into a dataframe
    Extended     |       mixed cases and St. and Street both                    |
                 |      2. state is not always two chars (i.e -Delaware)        |   2. Have a list of two chars state code vs state name property
                 |      3. facebook_id contains invalid value                   |      and replace any state name with state code.
                 |        (i.e @facebook.com)                                   |   3. Have a email validation utility and store "invalid"
                 |                                                              |   These steps have not been performed as the question on
                 |                                                              |   customer_extended does not require it to clean.
                 |                                                              |   Used  - upper(current_address)=="address_to_be_searched".toUpperCase
-----------------+--------------------------------------------------------------+-----------------------------------------------------------------------

Assumptions :
==================================================================================================================================================================

1. Spark Cluster should run spark version 2.1.0 or above and scala version 2.11.0 or above  as the code uses SparkSession and data frame api
2. To run on a YARN cluster , the data directory with all the files need to be put into HDFS
3. partition calculations and repartitioning has not been considered as the code was tested in local machine with 2 cpu cores.In cluster env,with increased number of
   executor-cores, sc.textFile() call should have a param denoting number of partitions for optimal performance.
4. persist(StorageLevel.MEMORY_AND_DISK) instead cache() has been used to cache dataframe for subsequent uses, persist with memory and disk is considered safest,
  though considering the size of data being handled, cache() (only memory) will work fine as well.
5. for question 4, net_purchase = total_amount-refund_amount(if any) has been considered for 2nd most calculation.
6. total_amount is the final amount used in calculation, total_quantity has not been multiplied with the total_amount for a transaction.

Execution Instructions [Step by Step Guide]
===================================================================================================================================================================
Local Machine in IDE (IntelliJ)

1. Download the project zip and extract into a local folder
2. Open IntelliJ and Go to File => New => Project from existing source and provide the path to the project root folder
3. Open the project in a new window.
4. [Optional] Right click on the project, Add Framework Support => Make sure scala and maven is checked. If not, check and follow on screen instructions.
5. Make data directory with all the files ready and available.
6. Go to top Menu Bar and select Run => Edit Configuration
7. On the Pop up window click on the green + button on top left and select "Application"
8. On the new window, give it a Name and select the following -
    a. "Main Class" will be AppleMain
    b.  "Program arguments" will be the fully qualified path of data dir.
         i.e - file:///C:/Users/371865/Downloads/coding test/coding test/data" in windows machine.
         Please make sure to put "file:///" and also whole path inside a ""(double quote) if dir name has space in it.

9. Run the AppleMain class. Go to top Menu Bar, select Run => Run and then select the name provided in step 8.
10. Results will be shown in the console.
Note : Please ignore this exception "java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries."
Error occurs because ${hadoop.home.dir} is not set and no hadoop installation present locally as well.


spark-submit on CLUSTER - YARN

1. put data files in a hdfs folder using hadoop fs -put <source folder on edge node> <hdfs folder>
2. build the artifact far to be run on spark cluster
        a. use coding-test-hpal-1.0-SNAPSHOT.jar provided in the target folder
        b. run "mvn -pdeployment clean install"  ( clean install with profile deployment) and coding-test-hpal-1.0-SNAPSHOT.jar
           will be created in ${build.dir}/target folder
3. copy the coding-test-hpal-1.0-SNAPSHOT.jar in the edge node of the cluster
4. locate the spark-submit executable on edge node and run the following command
    /usr/bin/spark-submit --master yarn  --class com.coding.de.AppleMain --num-executors 4 --executor-cores 2 --executor-memory 2g --driver-memory 2g /tmp/coding-test-hpal-1.0-SNAPSHOT.jar /user/hpal/data
    make sure to change the last 2 params to reflect correct value
    1st param => location of the coding-test-hpal-1.0-SNAPSHOT.jar on the server. (i.e - /tmp/coding-test-hpal-1.0-SNAPSHOT.jar)
    2nd param => full path of the data directory. (i.e /user/hpal/data).
5. Results will be show on edge node from where the spark-submit was executed.

Spark-Shell options
===================
Similarly, one can run on spark-shell as well. I've created a scripts for spark shell but i could not try it.
We dont need to create sparkContext, sc and spark variable will be available by default in spark shell.
We need to start the spark-shell with --jars options to make the Util, ModelFactory etc availale for import inside spark-shell
I've not tried but, one should start the spark-shell command as shown below -
/user/bin/spark-shell --jars /tmp/coding-test-hpal-1.0-SNAPSHOT.jar
this command will make all the required classes available for import.



Note
====================
I've tested spark-submit against 1.6.2 version of spark on YARN and with a different jar than the one that would be created from this workspace.
for 1.6.2 - I had to change the the following -
    a. spark.version=1.6.2 and scala.version=2.10.6 in POM xml
    b. Util.Enumerations had to be changed to smaller case as case class attribute names are small case, hence dataframe has small case,
        so this version of spark is case sensitive in terms of columns names when used in select,filter,where etc.
    c. Removed all reference to SparkSession and used sparkContext and sqlContext.



