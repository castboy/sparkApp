import kafka.serializer.StringDecoder  
import org.apache.spark.SparkConf  
import org.apache.spark.streaming.kafka.KafkaUtils  
import org.apache.spark.streaming.{Seconds, StreamingContext}  
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSON


object StreamingFirst {
	def main(args: Array[String]) {
		val brokers = "192.168.1.109:9092"  
		val topics = "xdr"  
  
		val sparkconf = new SparkConf().setAppName("kafkastreaming").setMaster("local[2]")  
		val ssc = new StreamingContext(sparkconf,Seconds(10))  
  
		ssc.checkpoint("w_checkpoints")
  
		val topicSet = topics.split(",").toSet  
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)  

		val lines = KafkaUtils.createDirectStream[String, String,StringDecoder, StringDecoder](ssc,kafkaParams,topicSet)  
		//val message = lines.map(_._1) map(_._1)  数据是空的 null  
		val message = lines.map(_._2) //map(_._2)  才是Kafka里面打入的数据
		val words = message.flatMap(_.split("\n"))
		//words.print()
		val dStream = words.transform(doHandleRdd(_))
		dStream.print()

		ssc.start()
		ssc.awaitTermination()  
	}  

        def doHandleRdd(rdd: RDD[String]) = {
            rdd.map(handleInfo(_))
        }

        def handleInfo(s: String) = {
            val b: Option[Any] = stringToJson(s)
            //println(b)
            b match {
                case Some(m2: Map[String, Any]) => {
                    println("Some:" + m2)
                    val t2 = m2.asInstanceOf[AnyRef].getClass.getSimpleName() //获取m2的数据类型
                    println("m2 type in Some:" + t2)

                    val m: Map[String, Any] = jsonToMap(b)

                    println("Map" + m)
                    val t = m.asInstanceOf[AnyRef].getClass.getSimpleName() //获取m的数据类型
                    println("m type:" + t)

                }
                case None => println("parsing failed.")
                case other => println("Unknown data structure: " + other)
            }
                 
        }

        def stringToJson(s: String): Option[Any] = {
            JSON.parseFull(s)
        }

        def jsonToMap(obj: Option[Any]): Map[String, Any] = {
            obj.get.asInstanceOf[Map[String, Any]]
        } 
}  
