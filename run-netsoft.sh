#/bin/bash
CURRFOLDER=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

spark-submit --driver-class-path /home/ubuntu/mysql-connector-java-5.1.37/mysql-connector-java-5.1.37-bin.jar --class "ca.savitestbed.monarch.graph.netsoft" --conf "spark.executor.memory=2g" --conf "spark.cores.max=5" --conf "spark.driver.memory=2g" --master spark://monarch-master:7077 $CURRFOLDER/../target/scala-2.10/monarch-spark-assembly-0.1.jar 'enter vmid here'
