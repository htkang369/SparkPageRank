import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import java.io._

object pageuni {

  //Define variables for start time, wikipedia data set location, Url and number of iterations for pagerank
  val t1 = System.currentTimeMillis()
  var wiki = "file:///home/freebase-wex-2009-01-12-articles.tsv"  // path
  var remoteURL = "local"
  val iterations = 3

  //Hash function to generate hash value based on title
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
    val sparkConf = new SparkConf().setAppName("pageuni").setMaster(remoteURL)
    val sc = new SparkContext(sparkConf)
    
    //Reading the wikipedia data set 
    val wikipedia = sc.textFile(wiki, 1)  // read 1 file from wiki
    case class Article(val id: Int, val title: String, val body: String)
    case class Connection(val source: Long, val destination: Long)
	 println("read lines")

    
	// university
	val univer = wikipedia.map(_.split('\t')).
	             filter(line=>line(1) contains "University")// univer is list
	
	//Interested in internal wikipedia links, hence as per WEX documentation, those within target tags are parsed  
    val findPattern = "<target>.+?<\\/target>".r
	val findPattern2 = "<name>.+?<\\/name>".r
     println("created pattern")	
    
	// val alumni is not used, ignore it 
	val alumni = univer.flatMap { a =>
		val sourceId = pageHash(a(1))
		findPattern2.findAllIn(a(3)).map { link =>  // link is string
        val destination = link.replace("<name>", "").replace("</name>", "")
		//val desfinal = if (destination.contains("alumni") || destination.contains("Alumni") || destination.contains("he") || destination.contains("his") || destination.contains("she")) destination else "no alumni"
        val desfinal = if (destination.length>0) destination else "no alunmi"
		(sourceId, desfinal)
		}
	}.distinct().groupByKey().cache()
		 
    val originalNames = univer.map { a =>
      val sourceId = pageHash(a(1)) // parts[1] is page title
      (sourceId, a(1))
    }.cache()
    
	// create input data for pagerank
    val links = univer.flatMap { a =>   // flatmap, output multiple elements
      val sourceId = pageHash(a(1))
      findPattern.findAllIn(a(3)).map { link =>
        val destinationId = pageHash(link.replace("<target>", "").replace("</target>", ""))
        (sourceId, destinationId)
      }
    }.distinct().groupByKey().cache()

	println("data preapred")
    //Set initial rank of every page to 1
    var setRanks = links.mapValues(v => 1.0)
	
	println("begin pagerank")

    for (i <- 1  to iterations) {
      val contribs = links.join(setRanks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(l => (l, rank / size))
      }
      setRanks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

	println("pagerank success")
    val result = originalNames.join(setRanks)
	val output = result.sortBy(x => x._2._2)
    val r = output.take(100).reverse // get first 10 elements
    val pw = new PrintWriter(new File("pageranksparkoutput.txt"))
    r.foreach(tup => (println(tup._2._1 + " has rank " + tup._2._2 +"."), pw.write(tup._2._1 + " has rank " + tup._2._2 + "." + "\n")))
    pw.close
    val t2 = System.currentTimeMillis()
    println("Time taken for PagerankSpark for " + iterations + " iterations is " + (t2-t1) + " msecs")
    sc.stop()
  }
}
