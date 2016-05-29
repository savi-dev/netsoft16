package ca.savitestbed.monarch.graph

import ca.savitestbed.monarch.fileprocessor._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import com.github.nscala_time.time.Imports._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._


/**
  * Created by ericlin on 15-08-24.
  * Modified by Byungchul Park
  */
object VmConnectionGraph {


  def createGraph(hdfspaths:String, vmid:String) {
    val sparkConf = new SparkConf()
      .setAppName("MultiLayerGraphGeneration")
    val sc = new SparkContext(sparkConf)
    sc.addJar("/home/ubuntu/mysql-connector-java-5.1.36/mysql-connector-java-5.1.36-bin.jar")

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val hdfsPathsArray = hdfspaths.split(",")
    val startTime = DateTime.now.minusHours(2).toString("YYYY-MM-dd HH:mm:ss")
    val endTime = DateTime.now.toString("YYYY-MM-dd HH:mm:ss")


    //get vm info (rid, project_id,user_id,hostname)
    val vmInfo = Utils.getVMuuidInfo(sqlContext)

    val ridToId = Utils.getVMRidToID(sqlContext)
    val idToRid = ridToId.map(ri => (ri._2.toLong, ri._1))

    // **** Section 1 Generate VM node (vmId, avgCPUUtil) ****
    val vmridAvgCPU = VmCPUUtil.calculateAvgFromFile(sqlContext, hdfsPathsArray, startTime, endTime)

    // (vmid, (rid, cpu_util, project, user, host))
    val vmidAvgCPU = vmridAvgCPU.join(
      vmInfo.map{
        case (rid,project,user,hostname) => (rid, (project,user,hostname))
      }).join(ridToId).map{
      case (rid, ((cpu_util, (project, user, host)), id))
      => (id, (rid, cpu_util, project, user, host))
    }


    
    // **** Section 2 Generate virtual link -- vm to vm connection ((srcVMId, dstVMId), BW) ****
    val commPairBwutil = OFFlowBw.calculateAvgPerCommPairFromFile(sqlContext, hdfsPathsArray, startTime, endTime)
    val macToVmrid = Utils.getMacToVMRid(sqlContext)

    // (rid, (mac, id))
    val ridToMacId = macToVmrid.map(mr => (mr._2, mr._1)).join(ridToId)
    val macToId = ridToMacId.map(rmi => rmi._2)

    // produce (source Mac, ((des MAC, average bandwidth), srcVMid))
    val convertedSrcMac:RDD[(String, ((String, Double), Option[Long]))] = commPairBwutil.map(p => (p._1, (p._2, p._3))).leftOuterJoin(macToId)

    // produce (dst Mac, ((srcVMId, average bandwidth), dstVMId))
    val convertedSrcDstMac:RDD[(String, ((Long, Double), Option[Long]))] = convertedSrcMac
      .map(p => p._2._2 match{
        case id:Some[Long] => (p._2._1._1, (id.get, p._2._1._2))
        case None => (p._2._1._1, (Utils.macToInt(p._1), p._2._1._2))
      })
      .leftOuterJoin(macToId)

    //((srcVMId, dstVMId), BW)
    val idToIdBw:RDD[((Long, Long), Double)] = convertedSrcDstMac
      .map(p => p._2._2 match{
        case id:Some[Long] => ((p._2._1._1, id.get), p._2._1._2)
        case None => ((p._2._1._1, Utils.macToInt(p._1)), p._2._1._2)})
      .reduceByKey((a, b) => a+b)

    val idToIdBwType:RDD[((Long, Long), Double, String)] = idToIdBw.map(f => (f._1, f._2, "vm_comm"))


    // Generate Graph

    val edges:RDD[Edge[(Double, String)]] = idToIdBwType.map(edgeRaw => Edge(edgeRaw._1._1, edgeRaw._1._2, (edgeRaw._2, edgeRaw._3)))
    val graph:Graph[ (String, Double, String, String, String), (Double, String)] = Graph(vmidAvgCPU, edges)

    //extracting subgraph contains intersted VM
    val targetVertexId = ridToId.lookup(vmid)(0)
    val newGraph = Graph(
      graph.vertices.filter{case (vid,attr) => vid == targetVertexId} ++
        graph.collectNeighbors(EdgeDirection.Either)
          .filter{ case (vid,arr) => vid == targetVertexId}
          .flatMap{ case (vid,arr) => arr},
      graph.edges
    ).subgraph(vpred = { case (vid,attr) => attr != null})

    val resultVertices = newGraph.vertices
    val resultEdges = newGraph.edges
   
    //convert data to JSON
    val vms = "vms" -> resultVertices.collect().toList.map{
      case(id,(rid, cpu, project, user, host))=>
        ("id", id) ~
          ("vmid", rid) ~
          ("cpu_utilization", cpu) ~
          ("project", project) ~
          ("user", user) ~
          ("hostname", host)
    }

    val links = "links" -> resultEdges.collect().toList.map{
      case(Edge(src,dst,(bw,com_type)))=>
      ("source", src) ~
        ("target", dst) ~
        ("bandwidth", bw)
    }

    val json = parse(compact(render(vms))) merge parse(compact(render(links)))

    import java.io._
    val pw = new PrintWriter(new File("/home/ubuntu/connection_"+vmid+".json"))
    pw.write(compact(render(json)))
    pw.close()

  }

  def main(args: Array[String]) {
    Class.forName("com.mysql.jdbc.Driver")

    if (args.length < 2) {
      System.err.println("Usage: MultiLayerGraphGeneration <hdfspaths> <start_time> <end_time>")
      System.exit(1)
    }

    // hdfspath is expected to be a folder path and this program selects the proper files to look into
    val hdfspaths = args(0)
    val vmid = args(1)
    createGraph(hdfspaths, vmid)
  }
}
