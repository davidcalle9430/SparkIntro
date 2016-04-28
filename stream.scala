import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

// Create a local StreamingContext with two working thread and batch interval of 1 second.

import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.entity.StringEntity;

// The master requires 2 cores to prevent from a starvation scenario.

val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
val ssc = new StreamingContext(sc, Seconds(30))
val lines = ssc.socketTextStream("localhost", 9999)
val words = lines.flatMap(_.split(" "))


object SecondValueOrdering extends Ordering [(String, Int)] {
  def compare(a: (String, Int), b: (String, Int)) = {
    b._2 compare a._2
  }
}

def sendData(word: String , count : Int ) = {
  val httpPost = new HttpPost("http://spring-server-galileoacmpuj.c9users.io/api/palabras")
  val json = "{ \"name\" : \"" + word + "\" , \"size\" : \""+ count +"\"  }";
  httpPost.setHeader("Accept", "application/json");
  httpPost.setHeader("Content-type", "application/json")
  val entity = new StringEntity(json);
  httpPost.setEntity( entity );
  val response = ( new DefaultHttpClient ).execute( httpPost )
}
// Count each word in each batch
val pairs = words.map( word => ( word , 1 ) )
val wordCounts = pairs.reduceByKey( _ + _ )

// Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.print()
wordCounts.foreachRDD { rdd =>
  rdd.takeOrdered( 1000 )( SecondValueOrdering ).foreach( a =>
     sendData( a._1 , a._2 )
   )
}


ssc.start()             // Start the computation
ssc.awaitTermination()  // Wait for the computation to terminate
