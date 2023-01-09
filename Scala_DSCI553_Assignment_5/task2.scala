import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object task2 {

  var hash_functions_n=100
  var windows_n=5
  var windowSize = (hash_functions_n/windows_n).toInt
  var estimate_val_sum = 0
  var ground_sum = 0
  val global_filter_bit_array_n=69997
  val p = scala.math.BigDecimal(1342757201)

  def main(args: Array[String]): Unit = {
//    val input_file="C:\\Users\\ankus\\PycharmProjects\\DSCI553_Assignment_5\\users.txt"
//    val stream_size=(300).toInt
//    val num_of_asks=(30).toInt
//    val output_file="task2.csv"

    val input_file=args(0)
    val stream_size=args(1).toInt
    val num_of_asks=args(2).toInt
    val output_file=args(3)


    var csv_output_tuple=mutable.ListBuffer[(Int,Int,Int)]()
    var count = 0
    var box = new Blackbox()
    for (i<- 0 to num_of_asks-1) {
      val stream_users = box.ask(input_file, stream_size)
      var final_value=flajolet_martin_algo(stream_users.toSet.toArray)
      for( i<-stream_users){
        print("'"+i+"',")
      }
      println()
      for( i<-stream_users.toSet.toArray){
        print("'"+i+"',")
      }
      var ground_truth_n_i=final_value._1
      println()
      println()
      var estimated_value_i=final_value._2
      csv_output_tuple+=Tuple3(count,ground_truth_n_i,estimated_value_i)
      count+=1
    }
    println((estimate_val_sum.toFloat/ground_sum.toDouble))
    var writer_file = new PrintWriter(new File(output_file))
    writer_file.write("Time,Ground Truth,Estimation"+"\n")
    for (out_i <- csv_output_tuple) {
      writer_file.write(out_i._1 + "," + out_i._2 + ","+ out_i._3 + "\n")
    }
    writer_file.close()
  }


  def myhashs(s: String): mutable.ListBuffer[BigDecimal] = {
    var result=mutable.ListBuffer[BigDecimal]()
    //    var str_s=""
    //    for(i <- s){
    //      str_s = str_s+i.toInt.toString
    //    }
    var str_s=BigDecimal(s.hashCode())
    for (f <- 0 to (hash_functions_n-1)){
      //        var temp_val=((a(f)*BigDecimal(str_s)+b(f))%p(f))%global_filter_bit_array_n
      var a = scala.util.Random.nextInt()
      var b = scala.util.Random.nextInt()
      var temp_val=((a*str_s+b)%p) % global_filter_bit_array_n
      if (temp_val<0){
        temp_val+=global_filter_bit_array_n
      }
      result+=temp_val
    }
    result
  }


  def flajolet_martin_algo(stream_users: Array[String]):(Int,Int)={
    var max_zeroes_trailing = new ListBuffer[Int]()
    for (i <- 0 to (hash_functions_n-1)){
      max_zeroes_trailing += 0
    }
    for (i <- stream_users){
      val x = myhashs(i)
      var bin_x = new ListBuffer[String]()
      var trailing_zeroes = new ListBuffer[Int]()
      for (y <- x){
        bin_x += y.toInt.toBinaryString
      }
      for (i <- bin_x){
        var first =i.length
        var second = i.replaceAll("0+$","").length
        trailing_zeroes += first - second
      }
      var zipped_max_trail =  trailing_zeroes zip max_zeroes_trailing
      for (i <- 0 to zipped_max_trail.length-1){
        var t = zipped_max_trail(i)
        if (t._1>t._2) {
          max_zeroes_trailing(i)=t._1.max(t._2)
        }
      }

    }
    var two_factor=new ListBuffer[Int]()
    for(i<- 0 to max_zeroes_trailing.length-1){
//      var temp=scala.math.pow(2,two_factor(i))
      two_factor+=scala.math.pow(2,max_zeroes_trailing(i)).toInt
    }

    two_factor=two_factor.sorted
    var sub_two_list=new ListBuffer[ListBuffer[Int]]()

    for(i<- 0 to windows_n-1){
      sub_two_list+=two_factor.slice(windowSize*i,((windowSize*(i+1))-1))
    }

    var average_two_factor = new ListBuffer[Int]()
    for(i<- 0 to sub_two_list.length-1){
      average_two_factor+=(sub_two_list(i).sum)/(sub_two_list(i).length)
    }
    var estimated_value = average_two_factor((average_two_factor.length/2).toInt).round
    estimate_val_sum=estimate_val_sum+estimated_value
    ground_sum=ground_sum+stream_users.length
    (stream_users.length,estimated_value)
  }

}
