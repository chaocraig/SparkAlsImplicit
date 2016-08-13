


import java.io.{PrintWriter, FileWriter}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark._
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, ALS, Rating}
import org.apache.spark.rdd.RDD
import org.dmg.pmml.True
//import org.uncommons.maths.statistics.DataSet

import scala.Option
import scala.collection.immutable.IndexedSeq
import scala.compat.Platform
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.util.Random

object SparkAlsImplicitPG {
//  //val log = LoggerFactory.getLogger(getClass)  //Spark system log
//  //val logger = Logger("Loggger Information")   //console log
//  private val RUN_JAR = "/Users/cray/Downloads/SparkAls/target/scala-2.10/sparkals_2.10-0.1-SNAPSHOT.jar"
//  private val Out_path = "/Users/cray/Downloads/SparkAls/output"
//  private val Out_Hadoop_Path = "hdfs://localhost:9000/user/cray/SparkAls/output"
//  //private val In_path  = "hdfs://localhost:9000/user/cray/SparkAls/ratings.dat"
  //private val In_path78  = "hdfs://localhost:9000/user/cray/SparkAls/pg_user_game_78_training_web.txt"
  //private val In_path_test78 = "hdfs://localhost:9000/user/cray/SparkAls/pg_user_game_78_test.txt"
  private val In_path90test_1  = "hdfs://localhost:9000/user/cray/tl2/cray_no_tl_test2.txt"
  private val In_path90test_2  = "hdfs://localhost:9000/user/cray/tl2/cray_not_only_tl_testing.txt"
  private val In_path90 = "hdfs://localhost:9000/user/cray/SparkAls/cray_web_user_game_nums_saving1.txt"
  //private val In_path_test90_s3 = "s3n://data.emr/test78ok.csv"
  //private val In_path90  = "hdfs://localhost:9000/user/cray/SparkAls/pg_user_game_90_training_v3.csv"
  private val In_path_test78 = "hdfs://localhost:9000/user/cray/SparkAls/als_web_not_only_king.csv"
  //private val In_path90  = "hdfs://localhost:9000/user/cray/SparkAls/pg_user_game_90_training_web_login_times.csv"
  //private val In_path_test90 = "hdfs://localhost:9000/user/cray/SparkAls/pg_user_game_90_test_01.txt"
//  //private val In_path = "hdfs://localhost:9000/user/cray/SparkAls/pg_user_game_90_training_has_login_days.txt"
//  //private val In_path_test = "hdfs://localhost:9000/user/cray/SparkAls/pg_user_game_90_test_has_login_days.txt"



  object Logger extends Serializable {
    val logger = LogManager.getRootLogger

    lazy val log = this.logger
  }

  case class RddDataSet(name: String, groupId:Int, train: RDD[Rating], test: RDD[Rating])

  object RddDataSet {
    def apply(name:String, train: String, test: String)(implicit sc: SparkContext): RddDataSet = {
      Logger.log.warn(s"DataSet:  $name, train:$train,\n test:$test")
      this(name, 0, text2tuples(sc.textFile(train), "training"),
        text2tuples(sc.textFile(test), "testing"))
    }
  }



  private val OutPath = "./78Saving1"
  private val OutPathPred = "./90Pred"


  private val MaxUniqueId: Int = 50000000
  private val MaxGameId:Int    = 220


  val runingHadoopConfig = new Configuration()
  val runingHdfs = FileSystem.get(runingHadoopConfig)

  def setSparkEnv(master: Option[String]): SparkContext = {
    val conf = new SparkConf()
    //.setMaster("local[16]")
    //.set("spark.executor.cores", "8")
//    conf.setMaster(master.getOrElse(s"local[${sys.runtime.availableProcessors()}]"))
    conf.setAppName("run-mark-ii")

    //.setAppName("SparkAls")
    // runtime Spark Home, set by env SPARK_HOME or explicitly as below
    //.setSparkHome("/opt/spark")

    // be nice or nasty to others (per node)
    //.set("spark.executor.memory", "1g")
    //.set("spark.core.max", "2")

    // find a random port for driver application web-ui
    //.set("spark.ui.port", findAvailablePort.toString)
    //.setJars(findJars)
    //.setJars(Seq(RUN_JAR))

    // The coarse-grained mode will instead launch only one long-running Spark task on each Mesos machine,
    // and dynamically schedule its own “mini-tasks” within it. The benefit is much lower startup overhead,
    // but at the cost of reserving the Mesos resources for the complete duration of the application.
    // .set("spark.mesos.coarse", "true")

    // for debug purpose
    //println("sparkconf: " + conf.toDebugString)

    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint/")
    sc.setLogLevel("WARN")
    val hConf = sc.hadoopConfiguration
    hConf.set("fs.s3.awsAccessKeyId", "")
    hConf.set("fs.s3.awsSecretAccessKey", "")
    sc
  }


  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx <= 0) {
        lines.drop(1)
      }
      lines
    })
  }


  def dropHeader(data: RDD[Rating], num:Int): RDD[Rating] = {
    val dataRdd: RDD[Rating] = data.mapPartitionsWithIndex((idx, lines) => {
      if (idx <= num) {
        lines.drop(1)
      }
      lines
    })
    dataRdd
  }



  def isNumeric(input: String): Boolean = input.forall(_.isDigit)

  case class ConfusionMatrixResult(accuracy: Double, precision: Double, recall: Double, fallout: Double, sensitivity: Double, specificity: Double, f: Double) {
    override def toString: String = {
      ("\naccuracy    = %02.4f\n" format accuracy) +
        ("precision   = %02.4f\n" format precision) +
        ("recall      = %02.4f\n" format recall) +
        ("fallout     = %02.4f\n" format fallout) +
        ("sensitivity = %02.4f\n" format sensitivity) +
        ("specificity = %02.4f\n" format specificity) +
        ("f           = %02.4f\n" format f)
    }


    def toListString(delimiter: String): String = {
      Logger.log.warn(s"toListString()...delimiter=$delimiter")
      (" %02.4f" format accuracy) + delimiter +
        ("%02.4f" format precision) + delimiter +
        ("  %02.4f" format recall) + delimiter +
        ("%02.4f" format fallout) + delimiter +
        ("%02.4f" format sensitivity) + delimiter +
        ("%02.4f" format specificity) + delimiter +
        ("%02.4f" format f)
    }

  }

  case class ConfusionMatrix(tp: Double = 0, fp: Double = 0, fn: Double = 0, tn: Double = 0)

  def calConfusionMatrix(data: RDD[((Int, Int), (Double, Double))]): ConfusionMatrixResult = {

    val confusionMatrix = data.flatMap {

      case ((user, product), (fact, pred)) if fact > 0 && pred > 0 ⇒
        Some(ConfusionMatrix(tp = 1))
      case ((user, product), (fact, pred)) if fact > 0 && pred <= 0 ⇒
        Some(ConfusionMatrix(fn = 1))
      case ((user, product), (fact, pred)) if fact <= 0 && pred > 0 ⇒
        Some(ConfusionMatrix(fp = 1))
      case ((user, product), (fact, pred)) if fact <= 0 && pred <= 0 ⇒
        Some(ConfusionMatrix(tn = 1))
      case _ ⇒
        Logger.log.warn(s"Error: confusionMatrix = $ConfusionMatrix")
        None
    }


    val result = confusionMatrix.reduce((sum, row) ⇒ ConfusionMatrix(sum.tp + row.tp, sum.fp + row.fp, sum.fn + row.fn, sum.tn + row.tn))


    Logger.log.warn(s"result = ${result}\n")
    val p = result.tp + result.fn
    val n = result.fp + result.tn
    Logger.log.warn(s"confusionMatrix: p=$p, n=$n\n")

    val accuracy = (result.tp + result.tn) / (p + n)

    val precision = result.tp / (result.tp + result.fp)
    val recall = result.tp / p
    val fallout = result.fp / n
    val sensitivity = result.tp / (result.tp + result.fn)
    val specificity = result.tn / (result.fp + result.tn)

    val f = 2 * ((precision * recall) / (precision + recall))

    ConfusionMatrixResult(accuracy, precision, recall, fallout, sensitivity, specificity, f)
  }


  private def ratingData(data: RDD[String]): RDD[Rating] = {
    Logger.log.warn("Rating...")

    case class Data(pubId: Int, gameId: Int, loginDays: Int, saving: Int)
    val header: String = data.first
    val parseData = data.filter(_ != header).map(_.split(",") match {
      case Array(pub_id, game_id, login_days, saving) => Data(pub_id.toInt, game_id.toInt, login_days.toInt, saving.toInt)
    })
    val dataSize = parseData.count
    val partSize = (dataSize / 5).toInt
    val sortByLoginDays = parseData.sortBy(_.loginDays).toLocalIterator.toList
    val sortBySaving = parseData.sortBy(_.saving).toLocalIterator.toList
    val loginDaysLevel1 = sortByLoginDays(partSize - 1).loginDays
    val loginDaysLevel2 = sortByLoginDays(partSize * 2 - 1).loginDays
    val loginDaysLevel3 = sortByLoginDays(partSize * 3 - 1).loginDays
    val loginDaysLevel4 = sortByLoginDays(partSize * 4 - 1).loginDays
    val savingLevel1 = sortBySaving(partSize - 1).saving
    val savingLevel2 = sortBySaving(partSize * 2 - 1).saving
    val savingLevel3 = sortBySaving(partSize * 3 - 1).saving
    val savingLevel4 = sortBySaving(partSize * 4 - 1).saving

    parseData map {
      case row =>
        val loginScore = row.loginDays match {
          case days if days <= loginDaysLevel1 => 0
          case days if days <= loginDaysLevel2 => 1
          case days if days <= loginDaysLevel3 => 2
          case days if days <= loginDaysLevel4 => 3
          case _ => 4
        }
        val savingScore = row.saving match {
          case saving if saving <= savingLevel1 => 0
          case saving if saving <= savingLevel2 => 1
          case saving if saving <= savingLevel3 => 2
          case saving if saving <= savingLevel4 => 3
          case _ => 4
        }

        val result = Rating(row.pubId, row.gameId, loginScore.toDouble / 100 + savingScore.toDouble)
        //Logger.log.warn("source = " + row)
        //Logger.log.warn("result = " + result)
        //Logger.log.warn("===")
        result
    }
  }


  def parseDouble(s: String) = {
    try {
      Some(s.toDouble)
    } catch {
      case _: Throwable => None
    }
  }

  def text2tuples(data: RDD[String], dataname: String): RDD[Rating] = {


    //ratings.data of MovieLens
    Logger.log.warn(s"Input $dataname size: ${data.count}\n");

    val header = data.first()
    Logger.log.warn(s"$dataname Mapping $header...\n");
    //val ratings = data.filter(_ != header).flatMap(_.split(",") match {
    val ratings = data.flatMap(_.split(",") match {
      //case Array(pub_id,game_id,gender,theme,style,community,type1,type2,mobile,saving) => {
      case Array(pub_id, game_id, str_saving, _*) => {
        //logger.warn("--->user: {}, item: {}, rate: {}\n", pub_id, game_id, saving )
        val game_id_no_quotes = game_id.replaceAll("\"", "")
        val option_saving = parseDouble(str_saving)

        for (s <- option_saving) yield {
          if (s >= 1000) {
            Rating(pub_id.toInt, game_id_no_quotes.toInt, 1)
          } else {
            Rating(pub_id.toInt, game_id_no_quotes.toInt, 0)
          }
        }

      }
      case some => //logger.warn("data error: {}", some.mkString(","));
        None
    }).persist()

    Logger.log.warn(s"$dataname size: ${ratings.count()}\n")
    ratings
  }


  case class singleAlsParameters(rank:Int=10, numIterations:Int=50, lambda:Double=0.01, alpha:Double=0.01){

     def toTuple: (Int, Int, Double, Double) = singleAlsParameters.unapply(this).get
  }


  case class singleAlsParameters3(rank:Int=10, lambda:Double=0.01, alpha:Double=0.01)


  def makeFilePath(OutPath:String, param:singleAlsParameters):String = {

    val (rank, numIterations, lambda, alpha) = param.toTuple

    Logger.log.warn("makePath()...")
    val path = "%s/%d_%d_%04.4f_%04.4f__%4d".format(s"${OutPath}",
      rank, numIterations, lambda, alpha, System.nanoTime % 10000)
    Logger.log.warn("path = " + path)

    path
  }


  def makePath(OutPath:String, name: String, param:singleAlsParameters): Path = {

    val (rank, numIterations, lambda, alpha) = param.toTuple

    Logger.log.warn("makePath()...")
    val path = "%s/%d_%d_%04.4f_%04.4f__%4d".format(s"${OutPath}_$name",
      rank, numIterations, lambda, alpha, System.nanoTime % 10000)
    Logger.log.warn("path = " + path)

    new Path(path)
  }



  def writeToFile(groupId:Int, name: String, param:singleAlsParameters, cf: ConfusionMatrixResult) {

    val (rank, numIterations, lambda, alpha) = param.toTuple

    val values = "%d, %d,%d,%04.4f,%04.4f,%s\n".format(groupId, rank, numIterations, lambda, alpha, cf.toListString(","))
    Logger.log.warn("\nvalues = " + values)

    val path = makePath(OutPath, name, param)

    if (runingHdfs.exists(path)) {
      Logger.log.warn(s"file runing exists: ${path.toString}")
    } else {

      val os = runingHdfs.create(path)
      val pw = new PrintWriter(os)
      try {
        pw.write(values)
      } finally {
        pw.close()
      }
    }
  }



  def hyperParameters(dataSet: RddDataSet, param:singleAlsParameters)  {

    val (rank, numIterations, lambda, alpha) = param.toTuple
    val groupId = dataSet.groupId

    Logger.log.warn("hyperParameters()...")
    Logger.log.warn(f"Training...groupId=$groupId, rank=$rank, numIterations=$numIterations, lambda=${lambda}%04.4f, alpha=$alpha%04.4f\n")
    //val model = ALS.trainImplicit(dataSet.train, rank, numIterations, lambda, alpha)
    val model: MatrixFactorizationModel = ALS.train(dataSet.train, rank, numIterations, lambda)

    // Evaluate the model on rating data
    val usersProducts = dataSet.test.map { case Rating(user, product, rate) =>
      (user, product)
    }
    Logger.log.warn(s"usersProducts.count() = ${usersProducts.count}\n")

    Logger.log.warn("Predicting...\n")
    val predictions =
      model.predict(usersProducts).map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    Logger.log.warn(s"predictions.count() = ${predictions.count}\n")

    Logger.log.warn("Joining...\n")
    val ratesAndPreds = dataSet.test.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(predictions).sortByKey() //ascending or descending
    Logger.log.warn(s"ratesAndPreds.count() = ${ratesAndPreds.count}\n")

    val MSE = ratesAndPreds.map { case ((_, _), (r1, r2)) =>
      math.pow(r1 - r2, 2)
    }.mean()

    Logger.log.warn(f"\n\n--->Mean Squared Error =  ${MSE}%4.4f  \n\n\n")

    val formatedRatesAndPreds = ratesAndPreds.map {
      case ((user, product), (rate, pred)) => user + "\t" + product + "\t" + rate + "\t" + "%02.4f".format(pred)
    }
    formatedRatesAndPreds.saveAsTextFile( makeFilePath(OutPath+"_Pred", param) )

    val cf = calConfusionMatrix(ratesAndPreds)
    Logger.log.warn(cf)

    writeToFile(groupId, dataSet.name, param, cf)

  }



  def nFoldTestHyperParameters(dataSet: RddDataSet, n:Integer, test_times:Int, param:singleAlsParameters) {

    //val datatest_new: Array[RDD[Rating]] = dataSet.test.randomSplit(Array.fill(n)(1/n), Platform.currentTime)
    Logger.log.warn(s"n=$n, dataSet = ${dataSet.test.count}")

    val data_size: Long = dataSet.test.count()
    val data_block_size: Long = data_size/n
    for (i <- 0L until (data_size-data_block_size) by data_block_size) { //n steps

      if (i < data_block_size*test_times) {

        val data_test: RDD[Rating] = dataSet.test.zipWithIndex.filter {
          case (data, id) => id >= i && id <= i + data_block_size - 1
        }.map(_._1)

        val data_test_others = dataSet.test.zipWithIndex.filter {
          case (data, id) => id < i || id > (i + data_block_size - 1)
        }.map(_._1)

        Logger.log.warn(s"data_test = ${data_test.count()}, data_test_others = ${data_test_others}")

        val rddDataSet = new RddDataSet(dataSet.name, dataSet.groupId, data_test_others, data_test)
        val model = hyperParameters(rddDataSet, param)

      }

    }

  }



  def nFoldHyperParameters(dataSet: RddDataSet, n:Integer, param:singleAlsParameters) {

    //val datatest_new: Array[RDD[Rating]] = dataSet.test.randomSplit(Array.fill(n)(1/n), Platform.currentTime)
    Logger.log.warn(s"n=$n, dataSet = ${dataSet.test.count}")

    val data_size: Long = dataSet.test.count()
    val data_block_size: Long = data_size/n
    for (i <- 0L until (data_size-data_block_size) by data_block_size) { //n steps

      val data_test: RDD[Rating] = dataSet.test.zipWithIndex.filter{
        case (data, id) => id >= i && id <= i + data_block_size - 1 }.map(_._1)

      val data_test_others = dataSet.test.zipWithIndex.filter{
        case (data, id) => id < i || id > (i + data_block_size - 1)  }.map(_._1)

      Logger.log.warn(s"data_test = ${data_test.count()}, data_test_others = ${data_test_others}")

      val rddDataSet = new RddDataSet(dataSet.name, dataSet.groupId, dataSet.train union data_test_others, data_test)

      hyperParameters(rddDataSet, param)

    }

  }



  def tryHyperParameters( dataSets: Traversable[RddDataSet])(implicit sc: SparkContext) = {

    val numIterations = 100
    val numPartitions = 100


    def logSpace(min: Double, max: Double, count: Int) = {
      (math.log(min) until math.log(max) by (math.log(max) - math.log(min)) / count).map(math.exp)
    }


    Logger.log.warn("Trying tryHyperParameters()...")



    val listAllParameters = for {
          lambda <- logSpace(0.01, 50, 100)
          alpha <- logSpace(0.01, 50, 100)
          rank <- 2 until 100 by 2
        } yield new singleAlsParameters(rank, numIterations, lambda, alpha)


    Logger.log.warn(s"Total ${listAllParameters.length} parameters")


    val tpool = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(3))

    sys.addShutdownHook {
      Logger.log.warn("\n\naddShutdownHook() Happened. Shutdown!\n\n")
      tpool.shutdown()
    }



    val count = new AtomicInteger(listAllParameters.size)
    val futures = Random.shuffle(listAllParameters).zipWithIndex.flatMap {

      case (param, groupId) ⇒

        val futureone = for (data <- dataSets) yield {

          Future {

            Logger.log.warn(s"groupId=$groupId, dataSets = ${dataSets.size}")
            Logger.log.warn(s"groupId=$groupId, ${data.name}: ${data.train.count()},${data.test.count()}")
            Logger.log.warn(s"groupId=$groupId, remaining=${count.getAndDecrement()}, choosen=$groupId\n")
            Logger.log.warn(s"groupId=$groupId, param=$param\n")


            val p = new singleAlsParameters(param.rank, 50, param.lambda, param.alpha)

            val Array(train_data, test_data) = data.train.randomSplit(Array(0.999, 0.001), Platform.currentTime)

            //val dataWithNewId = DataSet(data.name, groupId, train_data, test_data)
            //val test_data = sc.parallelize(data.train.take(3000))
            //val train_data: RDD[Rating] = dropHeader(data.train, 3000)
            Logger.log.warn(s"groupId=$groupId, test_data = ${test_data.count()}")
            Logger.log.warn(s"groupId=$groupId, train_data = ${train_data.count()}")

            val dataWithNewId = RddDataSet(data.name, groupId, train_data.repartition(numPartitions)
              , test_data.repartition(numPartitions) )


            // ---  for N-Fold Testing  ----
            //nFoldHyperParameters(dataWithNewId, 4, p)

            // ---  for N-Fold Testing, the training/testing datasets are from Test data  ----
            //nFoldTestHyperParameters(dataWithNewId, 1000, 1, p)

            // ---  for Single Random Testing ---
            //val dataWithNewId = RddDataSet(data.name, groupId, data.train, data.test)
            hyperParameters(dataWithNewId, p)

          }(tpool)
        }
        //Await.result(Future.sequence(futureone), Duration.Inf)
        futureone
    }
    Await.result(Future.sequence(futures), Duration.Inf)
  }


  def generateNullTrainData()(implicit sc: SparkContext):RDD[Rating] = {

    val nullTrainData =
      for (id <- 0 until MaxGameId)  yield {
         Rating(MaxUniqueId+id, id, 0)
      }
    sc.parallelize(nullTrainData)
  }

  def generateNullTestData()(implicit sc: SparkContext):RDD[Rating] = {

    val nullTestData =
      for (id <- 0 until MaxGameId)  yield {
        Rating(MaxUniqueId+id, 90, 0)
      }
    sc.parallelize(nullTestData)
  }


  def execModel(implicit sc: SparkContext) = {

    // Load and parse the data
    Logger.log.warn("\nLoad into RDD...\n");

    val Datas = Seq(
      RddDataSet("78", In_path90, In_path90test_1)
      //,RddDataSet("90_2", In_path90, In_path90test_2)

    )  //with sc

    //val d: DataSet = DataSet("90", In_path_test90, In_path_test90)

    val dataWithId = for (d <- Datas) yield {

      Logger.log.warn(s"dataWithId.train = ${d.train.count()}")
      Logger.log.warn(s"dataWithId.test  = ${d.test.count()}")
      //val newData: DataSet = DataSet(d.name, 0, d.train, d.test)

/*
      val nullTrainData = generateNullTrainData
      val nullTestData  = generateNullTestData
      Logger.log.warn(s"nullTrainData = ${nullTrainData.count()}")
      Logger.log.warn(s"nullTestData  = ${nullTestData.count()}")

      val allTrainData = d.train.union(nullTrainData)
      val allTestData  = d.test.union(nullTestData)
      Logger.log.warn(s"allTrainData = ${allTrainData.count()}")
      Logger.log.warn(s"allTestData  = ${allTestData.count()}")
      RddDataSet(d.name, 0, allTrainData, allTestData)
*/


      RddDataSet(d.name, 0, d.train, d.test)
    } //pass RDD instead path preventing redo

    tryHyperParameters(dataWithId)

  }


  def handleSparkParams(args: Array[String]):SparkContext = {
    setSparkEnv(args.lastOption)
  }


  def main(args: Array[String]) {

    implicit val sc = handleSparkParams(args)

    execModel

    sc.stop()
  }


}
