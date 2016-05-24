package ca.savitestbed.monarch.graph

import ca.savitestbed.monarch.fileprocessor._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.SQLContext

/**
 * Created by Joseph Wahba 
 * Modified by Bill Bateman
 *     - provides average cpu utilization for a given VM over the past 24h
 */
object netsoftCPU {

    def getAvgCpuUtil1h(hdfspaths:String, vmid:String) {
	    //create the spark context
		val sparkConf = new SparkConf().setAppName("cpuUtilization_"+vmid)
		val sc = new SparkContext(sparkConf)
		
		//add jar to spark context so all nodes in the cluster can access it
        sc.addJar("/home/ubuntu/mysql-connector-java-5.1.37/mysql-connector-java-5.1.37-bin.jar")
		//create sqlContext for dealing with data
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._ //necessary import for SQLContext
		
		//get the time range (last 24 hours)
		val endTime = DateTime.now.toString("YYYY-MM-dd HH:mm:ss")
		val startTime = DateTime.now.minusHours(1).toString("YYYY-MM-dd HH:mm:ss")
		
		//get cpu data from the given files
		val hdfsPathsArray = hdfspaths.split(",")
		val vmridAvgCPU = VmCPUUtil.calculateAvgFromFile(sqlContext, hdfsPathsArray, startTime, endTime)
		//calculateAvgFromFile returns an RDD of key-value pairs where rid is the key, cpu-util is the value
		
		//find the data for the given VM
		val avgCPU = vmridAvgCPU.lookup(vmid) //gives a list of all values with a key of 'vmid'
		
		//print the data
		if (avgCPU.length == 0) {
		    println("Error: No such VM found.")
			return
		}
		else if (avgCPU.length > 1) {
		    println("More than one instance found.")
		}
		//println("CPU Utilization for VM: " + vmid)
		//avgCPU.foreach(println)
		import java.io._
		val pw = new PrintWriter(new File("/home/ubuntu/cpuUtilization_"+vmid+".txt"))
		pw.write(avgCPU.mkString(""))
		pw.close()
		
	}
  
    def main(args: Array[String]) {
        Class.forName("com.mysql.jdbc.Driver") //forces the MySQL driver to load and initialize

        val hdfspaths = args(0) //should be a comma-separated list of paths
		                        //paths should be like: "hdfs://monarch-master/user/ubuntu/monitoring/parquet/METER_NAME_HERE/<year>/<month>/<day>/*/*.parquet"
		                        //we only need 24h of data, so just need the current day and the previous day
        val vmid = args(1)      //should be the resource id (rid) of a VM
		                        //e.g. 592d5faf-e0fc-42c4-8e91-66db97e5bd6f
        getAvgCpuUtil1h(hdfspaths, vmid)
    }
}
