import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce}
import org.apache.spark.mllib.recommendation._
import scala.util.control.Breaks._

case class UsrArt(user_id:Int, artist_id:Int, playcount:Double)
case class ArtAlias(bad_id: Int, good_id: Int)

object ArtistRecommender {
  def main(args: Array[String]): Unit = {
  
  val spark = SparkSession.builder().master("local[2]").
  appName("Artist Recommender").
  config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse").getOrCreate
  
  val sc = spark.sparkContext;
  
  import spark.implicits._;
  
  val user_artist = sc.textFile("hdfs://localhost:9000/hadoop-user/recommender/data/user_artist_data.txt")
             .map(x => x.split("\\s+"))
             .filter(x => x(0).matches("[0-9]+") && x(1).matches("[0-9]+")&& x(2).matches("[0-9]+"))
             .map(x => UsrArt(x(0).toInt,x(1).toInt,x(2).toDouble))
  
  val artist_alias = sc.textFile("hdfs://localhost:9000/hadoop-user/recommender/data/artist_alias.txt")
             .map(x => x.split("\\s+"))
             .filter(x => x(0).matches("[0-9]+") && x(1).matches("[0-9]+"))
             .map(x => ArtAlias(x(0).toInt,x(1).toInt))
             
  val user_artist_DF = user_artist.toDF
  val artist_alias_DF = artist_alias.toDF
  
  val user_artist_joined_DF = user_artist_DF.as("usrArt")
                    .join(artist_alias_DF.as("artAlias"),
                    $"usrArt.artist_id"===$"artAlias.bad_id","left_outer")
                    .select($"user_id",coalesce($"good_id",$"artist_id") as "artist_id",$"playcount")
  
  val user_artist_ratings_RDD = user_artist_joined_DF.as[(Int, Int, Double)].rdd
  val user_artist_ratings = user_artist_ratings_RDD.map{case(usr,art,play) => Rating(usr, art, play)}
  sc.setCheckpointDir("hdfs://localhost:9000/hadoop-user/recommender/checkpoint")
  val Array(training, testing) = user_artist_ratings.randomSplit(Array(0.8,0.2))
  val rank=20; val lambda=0.1; val alpha=1.0; val numiterations=20
  //val model = ALS.trainImplicit(training, rank, numiterations, lambda, alpha)
  val model = ALS.trainImplicit(training, rank, numiterations)
  val testinPred = testing.map{case Rating(user,product,rate) => (user,product)}
  val predicted = model.predict(testinPred).map{case Rating(user,product,rating) => (user,(product,rating))}
  val actual = testing.map{case Rating(user,product,rate) => (user,(product,rate))}
  val joined = actual.groupBy{case(x1,(x2,x3)) => x1}.join(predicted.groupBy{case(x1,(x2,x3)) => x1})
  val joined_f = joined.map(x => (x._1, x._2._1, x._2._2 )).map{case (x,y,z) => (x,extractIt(y),extractIt(z))}
  val usrrank = joined_f.map{case(usr, val1, val2) => find_rank(val1.toArray,val2.toArray)}
  val modelrank = usrrank.map(x => x).sum/usrrank.count()
  println("Model Rank: " + modelrank)
  
  }
  
  def extractIt(arr:Iterable[(Int,(Int,Double))]):Iterable[(Int,Double)]=
    { return arr.map(x => x._2)}
  
def find_rank(act:Array[(Int,Double)],pred:Array[(Int,Double)]):Double={
      val sortpred = pred.sortBy{case(x1,x2) => -x2}
      val calcpred = sortpred.zipWithIndex.map{case((x1,x2),x3) => (x1,(x3+1).toDouble/sortpred.size.toDouble)}
      val calcact_f = act.sortBy{case(x1,x2) => x1}
      val calcpred_f = calcpred.sortBy{case(x1,x2) => x1}
      var j = 0
      var upper = 0.0
      var lower = 0.0
      for ( i <- 0 to (calcpred_f.length - 1)){
        breakable{
            for ( j <- i to (calcact_f.length - 1)) {
                if(calcact_f(j)._1 == calcpred_f(i)._1){
                    upper += calcact_f(j)._2 * calcpred_f(i)._2
                    lower += calcact_f(j)._2
                    break
                }
            }
        }
      }
     return upper/lower
     }
  
}
