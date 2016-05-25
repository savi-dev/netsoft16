package ca.savitestbed.monarch.graph

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import com.github.nscala_time.time.Imports._
import org.apache.spark.sql.{SQLContext, DataFrame}

/**
 * Created by Bill Bateman
 *     - provides average cpu utilization for a given VM over the past 1h
 */
object netsoftCPU {
    val baseTableName = "VmTable_cpu_util"
    val baseCounterName = "cpu_util"

    def getTableName(startTime:String, endTime:String):String={
        //create table name from the base table name and start time and end time of the interested data
        val startTimeFormatted = startTime.replaceAll("[-:.]", "_").replaceAll(" ","_")
        val endTimeFormatted = endTime.replaceAll("[-:.]", "_").replaceAll(" ","_")
        val tableName = baseTableName + "_" + startTimeFormatted+"_to_" +endTimeFormatted
        tableName
    }


    def getData(sqlContext:SQLContext, hdfspaths:Array[String], startTime:String, endTime:String){
        //Function to get all the data for processing
        val tableName = getTableName(startTime, endTime)
        val convertedPath = hdfspaths.map(path => path.replace("METER_NAME_HERE", baseCounterName))
        val aggregatedData = sqlContext.read.parquet(convertedPath: _*).coalesce(10) //load data from file
        val dataFrame = aggregatedData.registerTempTable(tableName) //register tableName so we can access the data through sql
    }

    def getDataframeFromTable(sqlContext:SQLContext, hdfspaths:Array[String], startTime:String, endTime:String):DataFrame={
        //get data from file(s)
        getData(sqlContext, hdfspaths, startTime, endTime) 
        val tableName = getTableName(startTime, endTime) //can now use tableName to access the data

        //query the data for cpu data in the given time range
        val queryFields = "resource_id, counter_volume"
        val queryData = sqlContext.sql("select " + queryFields + " from " + tableName + " where counter_name='" + baseCounterName +
          "' and timestamp>='" + startTime + "' and timestamp<='" + endTime + "'" )
        queryData
    }

    def calulateAvgPerRid(df:DataFrame): RDD[(String, Double)] ={
        //computes the average for each VMid
        val volume_sum_per_vm = df.map(s => (s(0).toString(), (s(1).asInstanceOf[Double], 1)))
          .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)) //sums the values for each VMid, and keeps track of how many entries for each VM

        val volume_avg_per_vm = volume_sum_per_vm.map(vm => (vm._1, vm._2._1 / vm._2._2)) //for each VM, divide total value by number of entries to get average
        volume_avg_per_vm
    }

    def calculateAvgFromFile(sqlContext:SQLContext, hdfspaths:Array[String], startTime:String, endTime:String):RDD[(String, Double)]={
        //Read from hfds file and generate (rid, avg)
        val queryData = getDataframeFromTable(sqlContext, hdfspaths, startTime, endTime)
        val ridAvg = calulateAvgPerRid(queryData)
        ridAvg
    }

    def getAvgCpuUtil1h(hdfspaths:String, vmid:String) {
        //create the spark context
        val sparkConf = new SparkConf().setAppName("cpuUtilization_"+vmid)
        val sc = new SparkContext(sparkConf)

        //create sqlContext for dealing with data
        val sqlContext = new SQLContext(sc)
        import sqlContext.implicits._ //necessary import for SQLContext

        //get the time range (last 24 hours)
        val endTime = DateTime.now.toString("YYYY-MM-dd HH:mm:ss")
        val startTime = DateTime.now.minusHours(1).toString("YYYY-MM-dd HH:mm:ss")

        //get cpu data from the given files
        val hdfsPathsArray = hdfspaths.split(",")
        val vmridAvgCPU = calculateAvgFromFile(sqlContext, hdfsPathsArray, startTime, endTime)
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

        val hdfspaths = args(0) //should be a comma-separated list of paths
		                        //paths should be like: "hdfs://monarch-master/user/ubuntu/monitoring/parquet/METER_NAME_HERE/<year>/<month>/<day>/*/*.parquet"
		                        //we only need 1h of data, so just need the current day
        val vmid = args(1)      //should be the resource id (rid) of a VM
		                        //e.g. 592d5faf-e0fc-42c4-8e91-66db97e5bd6f
        getAvgCpuUtil1h(hdfspaths, vmid)
    }
}
