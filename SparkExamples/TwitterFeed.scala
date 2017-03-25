import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TwitterFeed {
  
  def main(args: Array[String]): Unit =
  {
    if (args.length < 2)
    {
      System.err.println("Usage: TwitterFeed <infile> <outfile>");
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TwitterFeed").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val twitter = sc.textFile(args(0)).map(x => x.split("\\s+"))
    val twitterA = twitter.map(x =>((x(0),x(1)),null))
    val twitterB = twitter.map(x =>((x(1),x(0)),null))
    val twitterJoin = twitterA.join(twitterB)
    val twitterFilter = twitterJoin.filter{case((key1,key2),value) => key1 < key2}
    val outTwitter = twitterFilter.map{case((key1,key2),value) => key1 + " " + key2}
    outTwitter.saveAsTextFile(args(1))
    sc.stop()
  }
  
}