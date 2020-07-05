import java.io._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer

object task1 {


  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("task21").setMaster("local[*]")
    var sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.FATAL)
    val s1=System.nanoTime()
    val case_ = args(0).toInt
    val support_1 = args(1).toInt
    val input1 = args(2)
    val output1 = args(3)

    var data = sc.textFile(input1)
    var header = data.first()
    data = data.filter(x => x != header)




    def getinformat[A : Ordering]
    (item: Seq[Iterable[A]]) = item.sorted

    var baskets_1: RDD[Set[String]] = null
    if (case_ == 1) {
      baskets_1 = data.map(x => x.split(",")).map(x => (x(0), x(1))).distinct().groupByKey().map(x => x._2.toSet)
      //baskets_1.foreach(println)

    } else {
      baskets_1 = data.map(x => x.split(",")).map(x => (x(1), x(0))).distinct().groupByKey().map(x => x._2.toSet)
    }

    var n = data.getNumPartitions

    def makepair(basket: Iterator[Set[String]], n: Int): Iterator[Set[String]] = {
      var pool = basket.toList
      var support_2 = (support_1 / n)
      var temp = pool.flatten.groupBy(identity).mapValues(_.map(_ => 1).sum).filter(x => x._2 >= support_2).map(x => x._1).toSet


      var final_temp = Set.empty[Set[String]]
      for (i <- temp) {
        final_temp = final_temp + Set(i)
      }

      var total = final_temp.size
      var temp_1 = Set.empty[String]
      var k = 2
      while (temp.size > 0) {
        if (k == 2) {
          var pairs = temp.subsets(2)
          for (i <- pairs) {
            breakable {
              var count = 0
              for (j <- pool) {
                if (i.subsetOf(j)) {
                  count = count + 1
                  if (count >= support_2) {
                    temp_1 = temp_1 ++ i
                    final_temp = final_temp + i
                    break
                  }
                }
              }
            }
          }
          k = k + 1
          temp = temp_1
          temp_1 = Set.empty[String]
        }
        else {
          //println("this is temp", temp, k)
          var tempu = temp.subsets(k)
          for (i <- tempu) {
            breakable {
              var count = 0
              for (j <- pool) {
                if (i.subsetOf(j)) {
                  count = count + 1
                  if (count >= support_2) {
                    temp_1 = temp_1 ++ i
                    final_temp = final_temp + i
                    break
                  }
                }
              }
            }
          }
          k = k + 1
          temp = temp_1
          //println("this is temp_1 before", temp_1)
          temp_1 = Set.empty[String]
          //println("this is temp_1 after", temp_1)

        }
      }

      return final_temp.iterator
    }

    var phase1 = baskets_1.mapPartitions(x => makepair(x, n)).distinct().map(x => (x, 1)).reduceByKey((x, y) => 1).map(_._1).collect()

    def finaltouch(basket: Iterator[Set[String]], ph: Array[Set[String]]): Iterator[(Set[String], Int)] = {
      var pool1 = basket.toList
      var outcome = List[(Set[String], Int)]()
      for (ii <- pool1)
      {
        for (jj <- ph)
        {
          if (jj.forall(ii.contains))
          {
            outcome = Tuple2(jj, 1) :: outcome
          }
        }
      }
      return outcome.iterator
    }: Iterator[(Set[String], Int)]

    var phase_2 = baskets_1.mapPartitions(x => finaltouch(x, phase1)).reduceByKey((a,b) => a+b).filter(_._2 >= support_1)
    var formatted_phase_2 = phase_2.map(x =>x._1).map(x => (x,x.size)).collect()

    var max = 0
    for (i <- formatted_phase_2)
    {
      if (i._2 > max)
      {
        max = i._2
      }
    }
    var single = getinformat(formatted_phase_2.filter(x => x._2 == 1).map(x => x._1))
    var output = "Candidates:\n"
    for (i<- single)
    {
      if (i==single.last){

        output += "('"+ i.toList.head + "')\n\n"
      }
      else {
        output += "('"+ i.toList.head + "')" + ","
      }
    }

    for (j <- 2 to max)
    {

      var size_ : Int= formatted_phase_2.size
      var internal_sorted=  ArrayBuffer[(List[String],Int)]()
      for(i_internal<- formatted_phase_2){
        internal_sorted .+=( Tuple2(i_internal._1.toList.sorted,i_internal._2))
      }
      var thisis = getinformat(internal_sorted.filter(x => x._2 == j).map(x => x._1))


      for (jj <- thisis) {
        var jjs = jj.toList.sorted
        output += "("
        for (zz <- jjs) {
          output += "'" + zz + "'" + "," + " "
        }
        output = output.slice(0, output.length - 2) + "),"
      }
      output = output.patch(output.lastIndexOf(','), "", 1) + "\n\n"

    }

    output += "Frequent Itemsets:\n"
    for (i<- single)
    {
      if (i==single.last){

        output += "('"+ i.toList.head + "')\n\n"
      }
      else {
        output += "('"+ i.toList.head + "')" + ","
      }
    }

    for (j <- 2 to max)
    {

      var size_ : Int= formatted_phase_2.size
      var internal_sorted=  ArrayBuffer[(List[String],Int)]()
      for(i_internal<- formatted_phase_2){
        internal_sorted .+=( Tuple2(i_internal._1.toList.sorted,i_internal._2))
      }
      var thisis = getinformat(internal_sorted.filter(x => x._2 == j).map(x => x._1))


      for (jj <- thisis) {
        var jjs = jj.toList.sorted
        output += "("
        for (zz <- jjs) {
          output += "'" + zz + "'" + "," + " "
        }
        output = output.slice(0, output.length - 2) + "),"
      }
      output = output.patch(output.lastIndexOf(','), "", 1) + "\n\n"

    }

    val printWriter = new PrintWriter(new File(output1))
    printWriter.write(output)
    printWriter.close()

    val s2=System.nanoTime()
    val t2=(s2-s1)/1000000000.0
    println("Duration:"+t2.toString())

  }

}

