package ca.savitestbed.monarch.graph

import ca.savitestbed.monarch.fileprocessor._
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import com.github.nscala_time.time.Imports._

/**
 * Created by Joseph Wahba 
 */
object netsoft {

  def createGraph(vmid:String): Unit ={
    //Some commands to initialize the Spark Job
    val sparkConf = new SparkConf()
      .setAppName(vmid)
    val sc = new SparkContext(sparkConf)
    sc.addJar("/home/ubuntu/mysql-connector-java-5.1.37/mysql-connector-java-5.1.37-bin.jar")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val currTime = DateTime.now.toString("YYYY_MM_dd_HH_mm_ss")
    //Generating the HDFS Paths where the Data is. We generate the paths of the data of the past 2 hours
    val hdfs1= DateTime.now.toString("YYYY/M/dd/HH")
    val hdfs2= DateTime.now.minusHours(1).toString("YYYY/M/dd/HH")
    val hdfs3= DateTime.now.minusHours(2).toString("YYYY/M/dd/HH")
    val hdfspaths = "hdfs://monarch-master/user/ubuntu/monitoring/parquet/METER_NAME_HERE/"+hdfs1+"/*,hdfs://monarch-master/user/ubuntu/monitoring/parquet/METER_NAME_HERE/"+hdfs2+"/*,hdfs://monarch-master/user/ubuntu/monitoring/parquet/METER_NAME_HERE/"+hdfs3+"/*"
    val hdfsPathsArray = hdfspaths.split(",") 
    //Setting the Start time as 2 hours ago and End time is now  
    val startTime = DateTime.now.minusHours(2).toString("YYYY-MM-dd HH:mm:ss")
    val endTime = DateTime.now.toString("YYYY-MM-dd HH:mm:ss")

    
    //Getting the VMs CPU utilization
    val ridToId = Utils.getVMRidToID(sqlContext) //produces (VMUUID,RID)
    val vmridAvgCPU = VmCPUUtil.calculateAvgFromFile(sqlContext, hdfsPathsArray, startTime, endTime) //produces(VMUUID,CPU utilization)
   
    //Getting the VM communication Links based on Mac addresses
    val commPairBwutil = OFFlowBw.calculateAvgPerCommPairFromFile(sqlContext, hdfsPathsArray, startTime, endTime) //produces (Source MAC, destination MAC, (discrete_time, average bandwidth), resource_id)
    val macToVmrid = Utils.getMacToVMRid(sqlContext)//Mapping between Mac Address and UUID
    //Filtering the flows to match the input VMid flows
    val macToVmrid_filtered = macToVmrid.filter(v =>v._2==vmid)//Gets the corresponding Mac for the input VM ID
    val vm_mac= macToVmrid_filtered.map(x =>x._1).collect()(0)//Converts the Mac in RDD to Mac in string format

    //Mapping the VM mac address to VM UUID
    val ridToMacId = macToVmrid_filtered.map(mr => (mr._2, mr._1)).join(ridToId) // (rid, (mac, RID))
    val macToId = ridToMacId.map(rmi => rmi._2) // (mac,RID)
    val convertedSrcMac:RDD[(String, ((String, Double), Option[Long]))] = commPairBwutil.map(p => (p._1, (p._2, p._3))).leftOuterJoin(macToId) // // produce (source Mac, ((des MAC, (discrete_time, average bandwidth)), srcVMid))
    //Getting the VMs connected to the input VM based on Mac addresses
    val convertedSrcMac_srcfiltered=convertedSrcMac.filter(v => v._1 == vm_mac || v._2._1._1 == vm_mac) 
    
    //Producing the Graph edges as source VM UUID and target VM UUID
    val mactomac= convertedSrcMac_srcfiltered.map(f => (f._1, f._2._1._1)).join(macToVmrid)
    val mactomac2= mactomac.map(f => f._2).join(macToVmrid)
    val vmidtovmid =  mactomac2.map(f => f._2) //produces Source VMID to Destination VMID 
    val edges =vmidtovmid // stores the edges pattern of the graph
	
    //Mapping the VM UUID to VM name
    val vmInfo = Utils.getVMuuidInfo(sqlContext).map(x => (x._1,x._4)) //produces all VMIDs with their names (VMID,VM name)
    val vmInfo2 = vmidtovmid.join(vmInfo)
    val src_name = vmInfo2.map(x =>(x._1,x._2._2)) // produce input VMID and connected VMs to it as sources and names (VMID,VM name)
    val vmInfo3 = vmidtovmid.join(vmInfo)
    val dst_name = vmInfo3.map(x=>x._2).join(vmInfo).map(x=> (x._1,x._2._2)) // produce input VMID and connected VMs to it as destinations and names (VMID,VM name)

    //Mapping the VM UUID to user ID
    val nodes_id=Utils.getVMuuidInfo(sqlContext).map(x => (x._1,x._3)) //produces all VMs UUIds and their corresponding User ID
    val nodes = src_name.union(dst_name).distinct()
    val nodes_name_cpu_user = nodes.join(vmridAvgCPU).join(nodes_id) // produces (VMID,((VM Name,CPU utilization),User ID))
    
    //Producing the Graph nodes as VMID, VM name, CPU utilization and User ID
    val nodes_formatted= nodes_name_cpu_user.map(x=> (x._1,x._2._1._1,x._2._1._2,x._2._2)) //produces (VMID,VM Name,CPU utilization,User ID)

    //Converting Graph from RDD format to JSON
    import org.json4s.native.JsonMethods._
    import org.json4s.JsonDSL.WithDouble._
    val nodesRdd= nodes_formatted
    val json = "VMs" -> nodesRdd.collect().toList.map{
    case (name, nodes,cpu,user) =>
	  ("VMID", name) ~
	  ("VMNAME", nodes) ~
	  ("CPU", cpu) ~
	  ("User", user) 
    }

    val nodes_string=(compact(render(json)))
    val edgesRdd= edges
    val json2 = "Links" -> edgesRdd.collect().toList.map{
    case (source,destination) =>
    ("Source", source) ~
    ("Target", destination)
    }  
    val edges_string=compact(render(json2))
    val graph_string= edges_string + nodes_string
    val graph_string2= edges_string.dropRight(1) +','+'\n' + nodes_string.drop(1)
    //Writing the graph to a file
    import java.io.File
    import java.io.PrintWriter
    import scala.io.Source
    val file_path =  "/home/ubuntu/"
    val writer = new PrintWriter(new File(file_path,vmid+"_"+currTime+".txt"))
    writer.write(graph_string2)
    writer.close()


  }

  def main(args: Array[String]) {
    Class.forName("com.mysql.jdbc.Driver")

    val vmid = args(0)
    createGraph(vmid)
  }
}
