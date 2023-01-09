import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.util.control.Breaks._


object task1 {
  var a = Seq.fill(8)(10000).map(scala.util.Random.nextInt)
  var b = Seq.fill(8)(10000).map(scala.util.Random.nextInt)
  var p=mutable.ListBuffer(70981, 70991, 70997, 70999, 71011, 71023, 71039, 71059)
//  println(a)
//  println(b)
//  println(p)

  var hash_functions_n=a.size

  var user_set=mutable.Set[String]()
  var result_count_fpr = mutable.ListBuffer[(Int, Double)]()
  val global_filter_bit_array_n=69997
  var global_filter_bit_array=mutable.ListBuffer[Int]()


  def main(args: Array[String]): Unit = {

    var counter=0
    val bx=new Blackbox()


    val input_file=args(0)
    val stream_size=args(1).toInt
    val num_of_asks=args(2).toInt
    val output_file=args(3)

//    val input_file="C:\\Users\\ankus\\PycharmProjects\\DSCI553_Assignment_5\\users.txt"
//    val stream_size=(100).toInt
//    val num_of_asks=(30).toInt
//    val output_file="task1.csv"

    for (i <- 1 to global_filter_bit_array_n){
      global_filter_bit_array+=0
    }

    for(k <- 0 to (num_of_asks-1)){
      var stream_users=bx.ask(input_file,stream_size)
      var fp_value_i=bloom_filter_func(stream_users)
      result_count_fpr+=Tuple2(counter, fp_value_i)
      counter=counter+1
    }
    var writer_file = new PrintWriter(new File(output_file))
    writer_file.write("Time,FPR"+"\n")
    for (fpr_i <- result_count_fpr) {
      writer_file.write(fpr_i._1 + "," + fpr_i._2 + "\n")
    }
    writer_file.close()
  }

  def bloom_filter_func(stream_users: Array[String]):Double={
    var tp_count=0
    var fp_count=0
  for (i<- stream_users) {
    var x = myhashs(i)
    var flag = 0
    breakable {
    for (k <- x) {
//      println(k)
      if (global_filter_bit_array(k.toInt) == 1) {
        flag = 1
      }
      else {
        flag = 0
        break
      }
    }
  }

    if (flag==1){
      if(!user_set.contains(i)){
        fp_count+=1
      }
      else{
        tp_count+=1
      }
    }
    else{
      for (jk<- x){
        global_filter_bit_array(jk.toInt)=1
      }
      user_set+=i
    }

    }
    var fp_value=0
    if ((tp_count+fp_count)==0) {
      var fp_value = 0.toDouble
    }
      else {
      var fp_value=(fp_count/(tp_count+fp_count)).toDouble
    }

    fp_value
  }


  def myhashs(s: String): mutable.ListBuffer[BigDecimal] = {
  var result=mutable.ListBuffer[BigDecimal]()
//    var str_s=""
//    for(i <- s){
//      str_s = str_s+i.toInt.toString
//    }
    var str_s=BigDecimal(s.hashCode())


//    val str_s=s.map(_.toByte).sum.abs
    for (f <- 0 to (hash_functions_n-1)){
//        var temp_val=((a(f)*BigDecimal(str_s)+b(f))%p(f))%global_filter_bit_array_n
var temp_val=((a(f)*(str_s)+b(f))%p(f))%global_filter_bit_array_n
      if (temp_val<0){
        temp_val+=global_filter_bit_array_n
      }

      result+=temp_val
    }
  result
  }
}

