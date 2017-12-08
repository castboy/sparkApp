import kafka.serializer.StringDecoder  
import org.apache.spark.SparkConf  
import org.apache.spark.streaming.kafka.KafkaUtils  
import org.apache.spark.streaming.{Seconds, StreamingContext}  
import org.apache.spark.rdd.RDD

import java.sql.{Connection, DriverManager, Statement}

import com.alibaba.fastjson.{JSON, JSONObject}
import java.util.Date

object KafkaToMysql {
	def main(args: Array[String]) {
            val brokers = "192.168.1.109:9092"  
            val topics = "xdr"  

            val sparkconf = new SparkConf().setAppName("kafkastreaming").setMaster("local[2]")  
            val ssc = new StreamingContext(sparkconf,Seconds(10))  

            ssc.checkpoint("w_checkpoints")

            val topicSet = topics.split(",").toSet  
            val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)  

            val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topicSet)  
            val message = lines.map(_._2) //map(_._2)  才是Kafka里面打入的数据
            val words = message.flatMap(_.split("\n"))

            val dStream = words.transform(doHandleRdd(_))

            dStream.foreachRDD{rdd => {
                    rdd.foreachPartition{partitionOfRecords =>
                        val stmt = mysqlConn()
                        partitionOfRecords.foreach{record => 
                            mysqlInsert(stmt, record)
                        }
                        connClose(stmt)
                    }
                }
            }

            ssc.start()
            ssc.awaitTermination()  
	}  

        def getNowStamp():Long = {
                val now = new Date()
                val a = now.getTime
                var str=a+""
                str.substring(0,10).toLong
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

        def mysqlInsert(stmt: Statement, json: JSONObject) {
            try{
                var sql = "insert into persion (name, age) values ('%s', %d)".format(json.get("name"), json.get("age"))
                val prep = stmt.execute(sql) 
                println("INSERT " + json + " OK")
            } catch {
                case e: Exception => println(e)
            }
        }

        def connClose(stmt: Statement) {
            stmt.close()
            println("conn close")
        }

        def doHandleRdd(rdd: RDD[String]): RDD[JSONObject]  = {
            println("-----***-----" + getNowStamp() + "-----***-----")
            rdd.map(handleInfo(_))
        }
        
        def handleInfo(s: String): JSONObject = {
            stringToJson(s) 
        }

        def stringToJson(s: String): JSONObject = {
            return JSON.parseObject(s)
        }
}  
