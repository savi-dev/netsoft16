#/bin/bash
CURRFOLDER=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
if [ "$#" -lt 1 ]; then
    echo "Illegal number of parameters, input the interested days"
    exit
fi

hourindate=$(seq 0 6)
#hourindate=$(seq 0 23)
dayinput=$1
days=(${dayinput//,/ })
targetfiles=()

echo "processing with days in May: "${days[*]}

for day in ${days[*]}
do
    for hour in ${hourindate[*]}
    do
        targetfiles+=("hdfs://monarch-master/user/ubuntu/monitoring/parquet/METER_NAME_HERE/2016/5/"$day/$hour/*)
    done
done

targetfilesss="${targetfiles[*]}"
targetfilesstr=${targetfilesss// /,}
#echo $targetfilesstr

spark-submit --driver-class-path /home/ubuntu/mysql-connector-java-5.1.37/mysql-connector-java-5.1.37-bin.jar --class "ca.savitestbed.monarch.graph.netsoft" --conf "spark.executor.memory=2g" --conf "spark.cores.max=5" --conf "spark.driver.memory=2g" --master spark://monarch-master:7077 $CURRFOLDER/../target/scala-2.10/monarch-spark-assembly-0.1.jar $targetfilesstr '2016-05-15 01:00:00' '2016-05-15 06:59:00' '592d5faf-e0fc-42c4-8e91-66db97e5bd6f'
