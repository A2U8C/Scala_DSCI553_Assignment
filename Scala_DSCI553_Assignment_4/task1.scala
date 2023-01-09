import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.graphframes.GraphFrame

import java.io.{File, PrintWriter}
import scala.collection.mutable

object task1 {
  def main(args: Array[String]): Unit = {
//    val input_path_val = "C:/Users/ankus/PycharmProjects/dsci544_Assignment-4/ub_sample_data.csv"
//    val communities_output_path_val = "Assignment-4_output_Realtask1_communities_output_Updated.txt"
//    val threshold_val = 7

    val input_path_val = args(1)
    val communities_output_path_val = args(2)
    val threshold_val = args(0).toInt


    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.master("local[*]").appName("HW4").getOrCreate()

    val sc = spark.sparkContext
    val sqlContext = new SQLContext(sc)
    val column_head = sc.textFile(input_path_val).first()
    val RDDFile = sc.textFile(input_path_val).filter(x => x != column_head).map(x => x.split(",")).map(x => (x(0), x(1)))
    val user_business_RDD = RDDFile.groupByKey().mapValues(_.toArray) //(user,[businesses])
    val user_business_Dict = user_business_RDD.collectAsMap() //user: [businesses]
    val all_users = user_business_RDD.map(x => x._1).collect() //[users]      #Nodes
    val user_pairs = all_users.combinations(2)
    val edges_user_list = mutable.ListBuffer[(String, String)]()
    val node_user_set = mutable.Set[(String, String)]()
    for (i <- user_pairs.toList) {
      val u_1 = user_business_Dict(i(0)).toSet
      val u_2 = user_business_Dict(i(1)).toSet
      if (u_1.intersect(u_2).size >= threshold_val) {
        edges_user_list += ((i(0), i(1)))
        edges_user_list += ((i(1), i(0)))
        node_user_set += ((i(0),i(0)))
        node_user_set += ((i(1),i(1)))
      }
    }
    val node_user_list=node_user_set.toList.map(x => Tuple1(x._1))


    val nodes_users = sqlContext.createDataFrame(node_user_list).toDF("id")
    val edges_users = sqlContext.createDataFrame(edges_user_list).toDF("src", "dst")



    val g = GraphFrame(nodes_users, edges_users)
    val results = g.labelPropagation.maxIter(5).run()

    val resultRDD = results.rdd.map(x => (x(1), x(0))).groupByKey().map(x => x._2.toList.map(_.toString).sorted).sortBy(x => (x.length,x(0))).collect()


        var pw = new PrintWriter(new File(communities_output_path_val))
        for (c <- resultRDD)
          for (i <- c) {
            pw.write("'" + i + "'")
            if (c.indexOf(i) == c.length - 1)
              pw.write("\n")
            else
              pw.write(", ")
          }
        pw.close()

  }

}
