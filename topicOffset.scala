package topicOffset

import java.sql.DriverManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.KafkaUtils

object metaDataStream extends App {
  val conf = new SparkConf().setMaster("local[*]").setAppName("kafka")
  val ssc = new StreamingContext(conf, Seconds(1))
  val url="jdbc:mysql://localhost:3306/<databasename>"
  val user="Username"
  val password="Password"
  val conn=DriverManager.getConnection(url,user,password)
  val statement = conn.createStatement()
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id" -> "LL",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
  val messagesDStream= KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String,String]
    (List("Topic1","Topic2","Topic3","Topic4","Topic5","Topic6"),kafkaParams))
  messagesDStream.foreachRDD{ rdd =>
        if (!rdd.isEmpty())
        {
          val topics=rdd.map(y=>(y.topic.toString)).take(1)
          val vTopic=topics(0)
          val partition=rdd.map(x=>(x.partition.toInt)).take(1)
          val vPartition=partition(0)
            var offsets=1
            val savedOffsetDF = statement.executeQuery("select * from topicPartitionOffsetTable where " +
              "topic = '" + vTopic + "' " +
              " and partition_number = " + vPartition)
            if (!savedOffsetDF.next()) {
              println("<------------INSERTION------------>")
              println("Topic---> '"+vTopic+"' with offset--->"+offsets)
              statement.executeUpdate("INSERT INTO topicPartitionOffsetTable(topic,partition_number,offset_number)"
                + " VALUES (" + "'" + vTopic + "'" + "," + vPartition + "," + offsets + ")")
            }
            else {
              val resultSet = statement.executeQuery("select offset_number from topicPartitionOffsetTable where topic='"+vTopic+"'")
              if(resultSet.next()) {
                 val offset= resultSet.getInt("offset_number")
                 offsets=offset+1
              }
              println("<------------UPDATION------------>")
              println("Topic---> '"+vTopic+"' with offset--->"+offsets)
              statement.executeUpdate("UPDATE topicPartitionOffsetTable SET "
                + "partition_number = " + vPartition + ", offset_number ="+ offsets +" "
                + "WHERE topic = '" + vTopic + "' "
                + "AND partition_number = " + vPartition)
            }
           }
        }
  ssc.start()
  ssc.awaitTermination()
}
