import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._

object TwittGraph {
  def main(args: Array[String]): Unit = 
  {
    if (args.length < 2)
    {
      System.err.println("Usage: TwitterGraph <infile> <outfile>");
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("TwitterGraph").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val edges = sc.textFile(args(0)).map(x => (x.split(" ")(0).toLong,x.split(" ")(1).toLong))
    val graph = Graph.fromEdgeTuples(edges,null)
    val gIndegree = graph.inDegrees
    val totalVert = graph.numVertices
    val indegree = gIndegree.map{case(vert, indeg) => (indeg, 1)}.reduceByKey((x1, x2) => x1 + x2)
    val indegreeDist = indegree.map{case(indeg, vertnum) => (indeg, vertnum.toDouble/totalVert.toDouble)}
    indegreeDist.saveAsTextFile(args(1))
  }
  
}
  
  