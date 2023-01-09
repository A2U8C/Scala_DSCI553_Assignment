import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.io.Source
import scala.util.Random

object task3  {
  Random.setSeed(553)
  var flagval=0
  var counter=100
  var history_list=mutable.ListBuffer[String]()
  var result_count_fpr = mutable.ListBuffer[(Int, mutable.ListBuffer[String])]()


  def main(args: Array[String]): Unit = {

    val bx=new Blackbox()

    val input_file=args(0)
    val stream_size=args(1).toInt
    val num_of_asks=args(2).toInt
    val output_file=args(3)


//    val input_file="C:\\Users\\ankus\\PycharmProjects\\DSCI553_Assignment_5\\users.txt"
//    val stream_size=(100).toInt
//    val num_of_asks=(30).toInt
//    val output_file="task3.csv"


    for(k <- 0 to (num_of_asks-1)){
      var stream_users=bx.ask(input_file,stream_size)
      var fp_value=fixed_sample_funct(stream_users)
      var fp_value_i=fp_value._2
      var counter_val=fp_value._1
      var fp_selected=mutable.ListBuffer(fp_value_i(0),fp_value_i(20),fp_value_i(40),fp_value_i(60),fp_value_i(80))
      result_count_fpr+=Tuple2(counter_val, fp_selected)
    }
    var writer_file = new PrintWriter(new File(output_file))
    writer_file.write("seqnum,0_id,20_id,40_id,60_id,80_id"+"\n")
    for (fpr_i <- result_count_fpr) {
      writer_file.write(fpr_i._1 + "," + fpr_i._2(0)+ "," + fpr_i._2(1)+ "," + fpr_i._2(2)+ "," + fpr_i._2(3)+ "," + fpr_i._2(4) + "\n")
    }
    writer_file.close()
  }

  def fixed_sample_funct(stream_users: Array[String]):(Int,mutable.ListBuffer[String])={
    if (flagval==1){
      for (j <- stream_users){
        counter+=1
        var temp_prob=(100.0/counter).toFloat
        var temp_random=Random.nextFloat()
        if(temp_random<temp_prob){
          var pos= Random.nextInt(history_list.size)
          history_list.update(pos,j)
        }
      }
    }
    else if(flagval==0){
      flagval=1
      for(i<-stream_users){
        history_list+=i
      }

    }
    (counter,history_list)
  }
}

class Blackbox{
  private var r1=scala.util.Random
  r1.setSeed(553)
  def ask(filename:String, num:Int):Array[String] = {
    var input_file_path=filename
    var lines = Source.fromFile(input_file_path).getLines().toArray
    var stream=new Array[String](num)
    for (i <- 0 to num-1){
      stream(i)=lines(r1.nextInt(lines.length))
    }
    stream
  }
}

