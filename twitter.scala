import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import scala.math.Ordering
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder

// Conf del systema
System.setProperty("twitter4j.oauth.consumerKey", getArgument("1. Consumer Key (API Key)", "JO8G5gHTMgsMsNs0RjuuiRgVP"))
System.setProperty("twitter4j.oauth.consumerSecret", getArgument("2. Consumer Secret (API Secret)", "fatsvxaRigHSdLX7WKMaxWbbqqWnHbOFTAICLE0AFDHSa5yLEa"))
System.setProperty("twitter4j.oauth.accessToken", getArgument("3. Access Token", "	2674238264-e3XVtCM0CavEvCGSjJovb8tQ5HnEqXZXt0GIZBB"))
System.setProperty("twitter4j.oauth.accessTokenSecret", getArgument("4. Access Token Secret", "	wWTpt7smKgLf2VbkKcK74xqYqD3jHW4Q9h0KwkROvya8z"))

//configuracion de donde se vana  guardar los datos
val outputDirectory = getArgument("1. Output Directory", "/twitter")
val slideInterval = new Duration( getArgument( "2. Recompute the top hashtags every N seconds", "1" ).toInt * 1000 )
val windowLength = new Duration(getArgument("3. Compute the top hashtags for the last N seconds", "5").toInt * 1000 )
val timeoutJobLength = getArgument("4. Wait this many seconds before stopping the streaming job", "100").toInt * 1000


dbutils.fs.rm( outputDirectory , true )

var newContextCreated = false
var num = 0

// This is a helper class used for
object SecondValueOrdering extends Ordering [(String, Int)] {
  def compare(a: (String, Int), b: (String, Int)) = {
    a._2 compare b._2
  }
}

// This is the function that creates the SteamingContext and sets up the Spark Streaming job.
def creatingFunc(): StreamingContext = {
  // Create a Spark Streaming Context.
  val ssc = new StreamingContext(sc, slideInterval)
  // Create a Twitter Stream for the input source.
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  val twitterStream = TwitterUtils.createStream(ssc, auth)

  // Parse the tweets and gather the hashTags.
  val hashTagStream = twitterStream.map(_.getText).flatMap(_.split(" ")).filter(_.startsWith("#"))

  // Compute the counts of each hashtag by window.
  val windowedhashTagCountStream = hashTagStream.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, windowLength, slideInterval)

  // For each window, calculate the top hashtags for that time period.
  windowedhashTagCountStream.foreachRDD(hashTagCountRDD => {
    val topEndpoints = hashTagCountRDD.top(10)(SecondValueOrdering)
    dbutils.fs.put(s"${outputDirectory}/top_hashtags_${num}", topEndpoints.mkString("\n"), true)
    println(s"------ TOP HASHTAGS For window ${num}")
    println(topEndpoints.mkString("\n"))
    num = num + 1
  })

  newContextCreated = true
  ssc
}

@transient val ssc = StreamingContext.getActiveOrCreate(creatingFunc)
ssc.start()
ssc.awaitTerminationOrTimeout(timeoutJobLength)
