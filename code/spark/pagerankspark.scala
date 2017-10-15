import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import java.io._

object pagerankspark {

  //Define variables for start time, wikipedia data set location, Url and number of iterations for pagerank
  val t1 = System.currentTimeMillis()
  //var wiki = "file:///home/subset.tsv"
  var wiki = "file:///home/freebase-wex-2009-01-12-articles.tsv"  // path to add files
  // var wiki = "file:///home/hadoop/data/data.txt"
  var remoteURL = "local"
  val iterations = 3

  //Use hash function to generate hash value based on title
  def pageHash(title: String) = {
    title.toLowerCase.replace(" ", "").hashCode.toLong   // lower all the letters,return hash code
  }

  def main(args: Array[String]) {

    //Check for the wikipedia customizable location and masterURL
    if (args.length >= 2) {
      remoteURL = args(1).toString.trim
    } else if(args.length >= 1) {
      wiki = args(0).toString.trim
    }else{
      //local url and local data set
    }

	 println("Setting spark context")
    //Setting spark context
    val sparkConf = new SparkConf().setAppName("PageRankSpark").setMaster(remoteURL)
    val sc = new SparkContext(sparkConf)

    //Reading the wikipedia data set
    val wikipedia = sc.textFile(wiki, 1)  // read 1 file from wiki
    case class Article(val id: Int, val title: String, val body: String)
    case class Connection(val source: Long, val destination: Long)
	 println("read lines")

    //Interested in internal wikipedia links, hence as per WEX documentation, those within target tags are parsed
    val findPattern = "<target>.+?<\\/target>".r
     println("created pattern")

    // split Wikipedia by tap
    val originalNames = wikipedia.map { a =>
      val parts = a.split('\t')
      val sourceId = pageHash(parts(1)) // parts[1] is page title,map part(1) to hash code
      (sourceId, parts(1))
    }.cache()

    val links = wikipedia.flatMap { a =>   // flatmap, input must be tuple
      val parts = a.split('\t')
      val sourceId = pageHash(parts(1))
      findPattern.findAllIn(parts(3)).map { link =>  // get all the contents between targets in xml
        val destinationId = pageHash(link.replace("<target>", "").replace("</target>", ""))
        (sourceId, destinationId)
      }
    }.distinct().groupByKey().cache()

	println("data preapred")
    //Set initial rank of every page to 1
    var setRanks = links.mapValues(v => 1.0) // set value as 1.0

	println("begin pagerank")

    for (i <- 1  to iterations) {
      val con = links.join(setRanks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(l => (l, rank / size))
      }
      setRanks = con.reduceByKey(_ + _).mapValues(0.12 + 0.88 * _)
    }

	println("pagerank success")
    val result = originalNames.join(setRanks) // [(sourceId,(source_name,rank))]
	  val output = result.sortBy(x => x._2._2) // assort according to rank value
    val r = output.take(100).reverse // get first 10 elements
    val pw = new PrintWriter(new File("pageranksparkout.txt"))
    r.foreach(tup => (println(tup._2._1 + " has rank " + tup._2._2 + "."), pw.write(tup._2._1 + "  " + tup._2._2 + "\n")))
    pw.close
	r.foreach(tup => (println(tup._2._1 + " has rank " + tup._2._2 + "."))
    val t2 = System.currentTimeMillis
    println("Time taken for PagerankSpark for " + iterations + " iterations is " + (t2-t1) + " ms")
    sc.stop()
  }
}