ㄊ# sbt "run -m $master "
# Spark Web UI:  http://host:8080
#
#export master="spark://ip-10-166-138-217.ap-southeast--1.compute.internal:7077"
export master="local[8]"
echo "Running on $master ..."
command="spark-submit \
   --driver-memory 12G \
   --class SparkAlsImplicitPG target/scala-2.10/sparkalsimplicitpg_2.10-1.0.jar \
   $master"
echo "Executing... $command"
`time $command`
