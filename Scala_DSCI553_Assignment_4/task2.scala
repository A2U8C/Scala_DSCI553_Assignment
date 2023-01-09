import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object task2 {
  def CommunitiesCreation_Function(nodes_edges_graphs: mutable.Map[String, mutable.Set[String]], node_user_list: mutable.Set[String]): ListBuffer[ListBuffer[String]] = {
    val final_communitites = ListBuffer[ListBuffer[String]]()
    var free_nodes = mutable.Set[String]()
    for (node_i <- node_user_list)
      free_nodes += node_i
    while (free_nodes.nonEmpty) {
      val visited_nodes = ListBuffer[String]()
      visited_nodes.append(free_nodes.toList.head)
      val temp_community = mutable.Set[String]()
      while (visited_nodes.nonEmpty) {
        val current = visited_nodes.remove(0)
        temp_community.add(current)
        for (neighbor <- nodes_edges_graphs(current))
          if (!temp_community.contains(neighbor))
            visited_nodes.append(neighbor)
      }
      val communityListBuffer = ListBuffer[String]()
      for (community_i <- temp_community)
        communityListBuffer += community_i
      final_communitites.append(communityListBuffer.sorted)
      free_nodes = free_nodes.diff(temp_community)
    }
    final_communitites
  }

  def shortest_path_Func(root: String, nodes_edges_graphs: mutable.Map[String, mutable.Set[String]]): (mutable.Map[String, Int],ListBuffer[String], mutable.Map[String, ListBuffer[String]]) = {
    val height_value = mutable.Map[String, Int]()
    val all_visited_nodes = ListBuffer[String]()
    all_visited_nodes.append(root)
    val shortestPath_var = mutable.Map[String, Int]()
    val node_buffer_queue = ListBuffer[String]()
    node_buffer_queue.append(root)
    val all_parent_nodes = mutable.Map[String, ListBuffer[String]]()
    for (vertex_node_i <- nodes_edges_graphs.keys) {
      all_parent_nodes(vertex_node_i) = ListBuffer[String]()
      shortestPath_var(vertex_node_i) = 0
      height_value(vertex_node_i) = -1
    }
    height_value(root) = 0
    shortestPath_var(root) = 1

    while (node_buffer_queue.nonEmpty) {
      val node_curr_i = node_buffer_queue.remove(0)
      val all_children_nodes = nodes_edges_graphs(node_curr_i)
      all_visited_nodes.append(node_curr_i)
      for (children_var_i <- all_children_nodes) {
        if (height_value(children_var_i) == -1) {
          node_buffer_queue.append(children_var_i)
          height_value(children_var_i) = height_value(node_curr_i) + 1
        }
        if (height_value(children_var_i) == height_value(node_curr_i) + 1) {
          shortestPath_var(children_var_i) = shortestPath_var(children_var_i) + shortestPath_var(node_curr_i)
          all_parent_nodes(children_var_i).append(node_curr_i)
        }
      }
    }
    Tuple3(shortestPath_var, all_visited_nodes, all_parent_nodes)
  }

  def Community_finder_Function(node_user_list: mutable.Set[String], nodes_edges_graphs: mutable.Map[String, mutable.Set[String]], betweenness: mutable.Map[(String, String), Double]): ListBuffer[ListBuffer[String]] = {
    var mod_global_value = -1.0
    var all_communitites = ListBuffer[ListBuffer[String]]()
    val final_node_edges = mutable.Map[String, mutable.Set[String]]() ++ nodes_edges_graphs
    var temp_betweenness_func = mutable.Map[(String, String), Double]() ++ betweenness
    val edges_count = betweenness.toList.length
    while (temp_betweenness_func.nonEmpty) {
      var totalModularity = 0.0
      val temp_communitites = CommunitiesCreation_Function(final_node_edges, node_user_list)
      for (community_node_i <- temp_communitites)
        for (i <- community_node_i)
          for (j <- community_node_i) {
            val size_i = nodes_edges_graphs(i).size
            val size_j = nodes_edges_graphs(j).size
            var a_ij = 0.0
            if (nodes_edges_graphs(i).contains(j))
              a_ij = 1
            totalModularity += a_ij - size_i * size_j / (2 * edges_count.toFloat)
          }
      val local_mod =totalModularity / (2 * edges_count.toFloat)
      if (local_mod > mod_global_value) {
        mod_global_value = local_mod
        all_communitites = temp_communitites
      }
      var removedEdges = ListBuffer[(String, String)]()
      val final_betweenness_ma: Double = temp_betweenness_func.values.max
      for (i <- temp_betweenness_func)
        if (i._2 == final_betweenness_ma)
          removedEdges += i._1
      for (edge <- removedEdges) {
        final_node_edges(edge._1) = final_node_edges(edge._1).filter(_ != edge._2)
        final_node_edges(edge._2) = final_node_edges(edge._2).filter(_ != edge._1)
      }
      temp_betweenness_func = Betweenness_Function(node_user_list, final_node_edges)
    }
    all_communitites
  }

  def Betweenness_Function(node_user_list: mutable.Set[String], nodes_edges_graphs: mutable.Map[String, mutable.Set[String]]): mutable.Map[(String, String), Double] = {
    val betweenness_final_value = mutable.Map[(String, String), Double]()
    for (user_node_i <- node_user_list) {
      val shortest_path_result_value = shortest_path_Func(user_node_i, nodes_edges_graphs)
      val edge_weight_calculator = mutable.Map[(String, String), Double]()
      val vertexWeight_temp_variable = mutable.Map[String, Double]()
      val shortestPath_temp_variable=shortest_path_result_value._1
      val visited_nodes_temp_variable = shortest_path_result_value._2
      val parents_temp_variable =shortest_path_result_value._3
      for (node_vert <- visited_nodes_temp_variable)
        vertexWeight_temp_variable(node_vert) = 1
      for (node_vert <- visited_nodes_temp_variable.reverse)
        for (parent_node_vert <- parents_temp_variable(node_vert)) {
          val pair_temp_variable = ListBuffer[String]()
          pair_temp_variable.append(node_vert)
          pair_temp_variable.append(parent_node_vert)
          val credit_temp_variable = (vertexWeight_temp_variable(node_vert) * shortestPath_temp_variable(parent_node_vert)) / shortestPath_temp_variable(node_vert)
          val orderedPair = Tuple2(pair_temp_variable.min, pair_temp_variable.max)
          if (!edge_weight_calculator.contains(orderedPair))
            edge_weight_calculator(orderedPair) = 0.0
          edge_weight_calculator(orderedPair) += credit_temp_variable
          vertexWeight_temp_variable(parent_node_vert) += credit_temp_variable
        }

      for (edge_weight_i <- edge_weight_calculator.keys) {
        if (!betweenness_final_value.contains(edge_weight_i))
          betweenness_final_value(edge_weight_i) = 0
        betweenness_final_value(edge_weight_i) += edge_weight_calculator(edge_weight_i) / 2
      }
    }
    //        for (edge_i_val <- betweenness.keys){
    //          betweenness(edge_i_val)=math.round(betweenness(edge_i_val)* 1000000 / 5).toFloat
    //          betweenness(edge_i_val)=betweenness(edge_i_val)* 5 / 1000000
    //        }
    betweenness_final_value


  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local[*]").appName("HW4").getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)

//        val input_path_val = "C:\\Users\\ankus\\IdeaProjects\\Assignment4_tasks\\Final_Assignment_5\\src\\main\\scala\\ub_sample_data.csv"
//        val betweenness_output_path_val = "Assignment-4_output_Realtask2_betweenness_output_Updated.txt"
//        val communities_output_path_val = "Assignment-4_output_Realtask2_communities_output_Updated.txt"
//        val threshold_val = 7

    val input_path_val = args(1)
    val betweenness_output_path_val = args(2)
    val communities_output_path_val = args(3)
    val threshold_val = args(0).toInt

    val sc = spark.sparkContext
    val column_head=sc.textFile(input_path_val).first()
    val RDDFile = sc.textFile(input_path_val).filter(x => x != column_head).map(x => x.split(",")).map(x => (x(0), x(1)))
    val user_business_RDD = RDDFile.groupByKey().mapValues(_.toArray) //(user,[businesses])
    val user_business_Dict = user_business_RDD.collectAsMap() //user: [businesses]
    val all_users = user_business_RDD.map(x => x._1).collect() //[users]      #Nodes
    val user_pairs = all_users.combinations(2)
    val edges_user_list=mutable.ListBuffer[(String,String)]()
    val node_user_list=mutable.Set[String]()
    for (i <- user_pairs.toList) {
      //        println(i(0),i(1))
      val u_1=user_business_Dict(i(0)).toSet
      val u_2=user_business_Dict(i(1)).toSet
      if (u_1.intersect(u_2).size>=threshold_val)
      {
        edges_user_list += ((i(0),i(1)))
        edges_user_list += ((i(1),i(0)))
        node_user_list+=i(0)
        node_user_list+=i(1)
      }
    }

    val nodes_edges_graphs = mutable.Map[String, mutable.Set[String]]()
    for (edge_i <- edges_user_list) {
      if (!nodes_edges_graphs.contains(edge_i._1))
        nodes_edges_graphs += (edge_i._1 -> mutable.Set[String]())
      nodes_edges_graphs(edge_i._1).add(edge_i._2)
      if (!nodes_edges_graphs.contains(edge_i._2))
        nodes_edges_graphs += (edge_i._2 -> mutable.Set[String]())
      nodes_edges_graphs(edge_i._2).add(edge_i._1)
    }

    // Betweenness: (String,String,Double)
    val betweenness = Betweenness_Function(node_user_list, nodes_edges_graphs)
    // Communities determination and printing
    var community_detector_result = Community_finder_Function(node_user_list, nodes_edges_graphs, betweenness)


    //Writing methods for betweenness and community
    var writer_file = new PrintWriter(new File(betweenness_output_path_val))
    val betweenness_List = betweenness.toList.map(x => (x._1._1, x._1._2, x._2)).sortBy(x => (-(x._3.toDouble), x._1, x._2))
    for (betweenness_i <- betweenness_List) {
      var ans_value=math.round(betweenness_i._3* 100000 ).toDouble
      ans_value=ans_value / 100000
      writer_file.write("('" + betweenness_i._1 + "', '" + betweenness_i._2 + "'), " + ans_value + "\n")
    }
    writer_file.close()

    writer_file = new PrintWriter(new File(communities_output_path_val))
    community_detector_result = community_detector_result.sortBy(x => (x.size, x.toString()))
    for (community_i <- community_detector_result)
      for (elem_community_i <- community_i) {
        writer_file.write("'" + elem_community_i + "'")
        if (community_i.indexOf(elem_community_i) == community_i.size - 1)
          writer_file.write("\n")
        else
          writer_file.write(", ")
      }
    writer_file.close()
  }


}