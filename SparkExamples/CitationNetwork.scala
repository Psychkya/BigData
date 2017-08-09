import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf._
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input._
import util.hashing.{MurmurHash3}
import org.apache.spark.graphx._

object CitationNetwork {
  def main(args: Array[String]): Unit = {
    //set spark context
    val spark = SparkSession
                .builder().master("yarn")
                .appName("Artist Recommender")
                .config("spark.sql.warehouse.dir","hdfs://BD-HM18:9000/user/hive/warehouse").getOrCreate
                
    val sc = spark.sparkContext;
    
    //set checkpoint directory
    sc.setCheckpointDir("hdfs://BD-HM18:9000/hadoop-user/spark/checkpoint")
    
    //parse input file
    @transient val hadoopConf = new Configuration
    hadoopConf.set("textinputformat.record.delimiter","#*")
    val inputRDD = sc.newAPIHadoopFile("hdfs://BD-HM18:9000/hadoop-user/final-project/data/citation-acm-v8.txt",classOf[TextInputFormat],classOf[LongWritable],classOf[Text],hadoopConf).map{case(key,value) => value.toString}.filter(value => value.length!=0).filter(x => x.contains("#index"))
    val inputRDD_1 = inputRDD.map(x => x.split("#index")).map(x => x(1)).map(x => x.split("#!")).map(x => x(0)).filter(x => x.contains("#%"))
    val inputRDD_2 = inputRDD_1.map(x => x.split("#%",2)).map(x => (x(0),x(1)))
    val inputRDD_3 = inputRDD_2.flatMapValues(x => x.split("#%"))
    val cite_graph = inputRDD_3.map{case (x,y) => (Math.abs(MurmurHash3.stringHash(x)).toLong, Math.abs(MurmurHash3.stringHash(y).toLong))}
    
    //create graph object and generate indegree distribution   
    val graph = Graph.fromEdgeTuples(cite_graph,null)
    val gIndegree = graph.inDegrees
    val verts = graph.numVertices
    val indegree = gIndegree.map{case(vert, indeg) => (indeg, 1)}.reduceByKey((x1, x2) => x1 + x2)
    val indegreeDist = indegree.map{case(indeg, vertnum) => (indeg, vertnum.toDouble/verts.toDouble)}
    indegreeDist.saveAsTextFile("hdfs://BD-HM18:9000/hadoop-user/final-project/output/A-IndegreeDist")
    
    //find weighted page rank
    val gOutdegree = graph.outDegrees
    val in_out = gIndegree.join(gOutdegree)
    val refG = cite_graph.map{case(x,y) => (y,x)}
    val cite_getlinks = refG.join(in_out).map{case(a,(b,(c,d))) => (b,(c,d))}.groupByKey
    val cite_mapgraph = cite_graph.join(cite_getlinks).map{case(a,(b,(c))) => (b,(a,(c)))}
    val cite_getnodelinks = cite_mapgraph.join(in_out)
    val cite_trans = cite_getnodelinks.map{case(ref,((cite,(links)),(in,out))) => (cite,(ref,links,in,out))}
    var rank = in_out.map{case(a,(b,c)) => (a,1/verts.toDouble)}
    for (j <- 0 to 9){
      val cite_final = cite_trans.join(rank).map{case(cite,((ref,links,in,out),rnk)) => (cite,ref,links,in,out,rnk)}
      cite_final.checkpoint()
      val contrib = cite_final.map{case(cite,ref,links,in,out,rnk) => (ref,getWRank(links.toArray,in,out,rnk))}
      rank = contrib.reduceByKey((c1,c2) => c1+c2).map{case(a,b) => (a,b * 0.85 + (0.15/verts))}
      rank.checkpoint()
    }
    val rank_sorted = rank.sortBy(-_._2)
    rank_sorted.saveAsTextFile("hdfs://BD-HM18:9000/hadoop-user/final-project/output/SortedRank")
    
    //Get final output
    val input_title = inputRDD.map(x => x.split("#index")).map(x => (x(0),x(1))).map{case(x,y) => (x.split("#",2)(0),y.split("#%",2)(0))}
    val input_title_final = input_title.map{case(x,y) => (Math.abs(MurmurHash3.stringHash(y)).toLong, x)}
    val top10_papers = sc.parallelize(rank_sorted.take(10))
    val top10_papers_title = top10_papers.join(input_title_final)
    val top10_papers_title_indegree = top10_papers_title.join(gIndegree).map{case(vert,((rnk,title),in)) => (title, in, rnk)}
    top10_papers_title_indegree.saveAsTextFile("hdfs://BD-HM18:9000/hadoop-user/final-project/output/Top10Papers")
    
    
  }
  //routine to calculate page rank
  def getWRank(wl:Array[(Int,Int)],inNode:Int, outNode:Int, rNode:Double):Double={
      var iLower = 0.0
      var oLower = 0.0
      for (i <- 0 to wl.length -1){
            iLower += wl(i)._1
            oLower += wl(i)._2
      }
      return ((inNode/iLower)*(outNode/oLower) * rNode)
  }
}