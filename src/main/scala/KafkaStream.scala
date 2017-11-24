import kafka.serializer.StringDecoder  
import org.apache.spark.SparkConf  
import org.apache.spark.streaming.kafka.KafkaUtils  
import org.apache.spark.streaming.{Seconds, StreamingContext}  

/** 
  * Created by guoshuai on 2017/11/15. 
  */  
class KafkaStream {  
  
}  

object KafkaStream {
	def main(args: Array[String]) {
		val brokers = "192.168.1.107:9092"  
		val topics = "xdr"  
  
		val sparkconf = new SparkConf().setAppName("kafkastreaming").setMaster("local[2]")  
		val ssc = new StreamingContext(sparkconf,Seconds(5))  
  
		ssc.checkpoint("w_checkpoints")
  
		val topicSet = topics.split(",").toSet  
		val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)  
  
  
                //lines是(null, topic-data)元组
		val lines = KafkaUtils.createDirectStream[String, String,StringDecoder, StringDecoder](ssc,kafkaParams,topicSet)  
                lines.print() 

		//val message = lines.map(_._1) map(_._1)  数据是空的 null  
		val message = lines.map(_._2) //map(_._2)  才是Kafka里面打入的数据
                message.print()

		val words = message.flatMap(_.split(":"))
		words.print()
		//outs = words.transform(x.map(x => (x.length,x)))
		//outs.print()
		//val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
		//wordCounts.print()  
		//message.print()  checked  

		ssc.start()  
		ssc.awaitTermination()  
	}  
}  
