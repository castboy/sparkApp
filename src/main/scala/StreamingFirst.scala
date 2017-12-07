import kafka.serializer.StringDecoder  
import org.apache.spark.SparkConf  
import org.apache.spark.streaming.kafka.KafkaUtils  
import org.apache.spark.streaming.{Seconds, StreamingContext}  
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON
import java.sql.{Connection, DriverManager, Statement}

object StreamingFirst {
	def main(args: Array[String]) {
           
            val brokers = "192.168.1.109:9092"  
            val topics = "xdr"  

            val sparkconf = new SparkConf().setAppName("kafkastreaming").setMaster("local[2]")  
            val ssc = new StreamingContext(sparkconf,Seconds(10))  

            ssc.checkpoint("w_checkpoints")

            println("ssc ok")
            val topicSet = topics.split(",").toSet  
            val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)  

            val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topicSet)  
            println("DStream ok")
            val message = lines.map(_._2) //map(_._2)  才是Kafka里面打入的数据
            val words = message.flatMap(_.split("\n"))
            println("words ok")

            val dStream = words.transform(doHandleRdd(_))
            println(dStream)
            dStream.foreachRDD{rdd => {
                    rdd.foreachPartition{partitionOfRecords =>
                        val stmt = mysqlConn()
                        partitionOfRecords.foreach{record => 
                            mysqlInsert(stmt, record)
                            println(record)
                        }
                        connClose(stmt)
                    }
                }
            }

            ssc.start()
            ssc.awaitTermination()  
	}  
       
        def mysqlConn(): Statement ={
            var dbConn: Connection = null
            var statement : Statement = null

            val url = "jdbc:mysql://10.88.1.102:3306/wmg"     
            val driver = "com.mysql.jdbc.Driver"
            val username = "root"
            val password = "mysqladmin"

            try {
                Class.forName(driver)
                dbConn = DriverManager.getConnection(url, username, password)
                statement = dbConn.createStatement()
            } catch {
                case e: Exception => {
                    println(e)
                }
            }
            println("conn mysql ok")    

            statement
        }

        def mysqlInsert(stmt: Statement, map: Map[String, Any]) {
            try{
                var sql = "insert into persion (name, age) values ('%s', %d)".format(map.getOrElse("name", ""), map.getOrElse("age", 0))
                println(sql)
                val prep = stmt.execute(sql) 
            } catch {
                case e: Exception => println(e)
            }
        }

        def connClose(stmt: Statement) {
            stmt.close()
            println("conn close")
        }

        def doHandleRdd(rdd: RDD[String]): RDD[Map[String, Any]]  = {
            println("doHandleRdd")
            rdd.map(handleInfo(_))
        }

        
        def handleInfo(s: String): Map[String, Any] = {
            val o: Option[Any] = stringToJson(s)
            var ret: Map[String, Any] = Map("name"->"wmg", "age"->27)
            ret
            /*o match {

                case Some(map: Map[String, Any]) => {
                   //ret = loopMap(map)
                   //ret = jsonToMap(o)
                   //ret = map
                }
                case None => {
                    println("parsing failed.")
                }
                case other => {
                    println("Unknown data structure: " + other)
                }

                ret
            }
            */
        }


        /*def loopMap(map: Map[String, Any]): Map[String, Any] = {
            val m: Map[String, Any] = map

            for ((x, y) <- map) {
                y match {
                    case s: String => println("string:" + s)
                    case i: Int => println("int:" + i)
                    case d: Double => println("double:" + d)
                    case b: Boolean => println("boolean:" + b)
                }
            }
            
            m
        }*/

        def stringToJson(s: String): Option[Any] = {
            JSON.parseFull(s)
        }

        def jsonToMap(obj: Option[Any]): Map[String,Any] = {
                obj.get.asInstanceOf[Map[String,Any]]
        }

}  
