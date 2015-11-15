import akka.actor._
import java.net._
import java.io._
import scala.io._
import java.math.BigInteger
import java.net.ServerSocket
import java.security.MessageDigest
import java.util.Random
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.concurrent.Await
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout
import scala.math._
object Master {
  case class Find_successor(hid: BigInt)
  case class Closest_preceding_finger(hid:BigInt)
  case class Finger(x:Int)
  case class U_Finger(x:Int,ref:ActorRef,hash:BigInt)
  case class U_Predecessor(ref:ActorRef,hash:BigInt)
  case class Finger_s_n_r(x:Int)
  case class Node_num(x:Int)
  case class Request_num(x:Int)
  case class Message(jump:Int,dest_id:BigInt,str:String)
  //case class Find_successor(ref:ActorRef,id:BigInt,dest_id:BigInt)
}

class Node(num: Int,m:ActorRef) extends Actor {
  val debug = false //the other debug flag is in line 191
  import Master._
  val int_id = num
  var data:List[String] = List()
  var ini = false

  implicit val timeout = Timeout(10 seconds)
  val master:ActorRef = m
  val max_bits = 160
  val md = java.security.MessageDigest.getInstance("SHA-1")
  val hash_str:String = md.digest(num.toString.getBytes("UTF-8")).map("%02x".format(_)).mkString
  val hash_id:BigInt = hash_str.toList.map( "0123456789abcdef".indexOf(_)).map( BigInt(_)).reduceLeft( _ * 16 + _)
  println(num+" -> "+hash_id)

  // finger_table
  // [start,interval_start,interval_end, first_node, first_node_ref]
  val finger_table = new Array[(BigInt,BigInt,BigInt,BigInt,ActorRef)](max_bits)
  // predecessor
  var pre_id:BigInt = null
  var pre_ref:ActorRef = null

  if(int_id == 0){
    //println("creating first node "+int_id)
    for(i <- 0 until max_bits){
      val start:BigInt = (hash_id + BigInt(2).pow(i)).mod(BigInt(2).pow(max_bits))
      val interval_start:BigInt = start
      val interval_end:BigInt = (hash_id + BigInt(2).pow(i+1)).mod(BigInt(2).pow(max_bits))

      var first_node:BigInt = hash_id

      finger_table(i) = (start,interval_start,interval_end, first_node, self)
    }
    pre_id = hash_id
    pre_ref = self
  }else{
    //println("creating first node "+int_id)
    for(i <- 0 until max_bits){
      val start:BigInt = (hash_id + BigInt(2).pow(i)).mod(BigInt(2).pow(max_bits))
      val interval_start:BigInt = start
      val interval_end:BigInt = (hash_id + BigInt(2).pow(i+1)).mod(BigInt(2).pow(max_bits))

      var first_node:BigInt = null

      finger_table(i) = (start,interval_start,interval_end, first_node, null)
    }
  }



  def u_finger(x:Int,ref:ActorRef,hash:BigInt):Boolean = {
    finger_table(x) = finger_table(x).copy(_4=hash,_5=ref)
    return true
  }

  def u_pre(ref:ActorRef,hash:BigInt):Boolean = {
    pre_ref = ref
    pre_id = hash
    return true
  }

  def get_finger_s_n_r(x:Int):(BigInt,BigInt,ActorRef) = {
    return (finger_table(x)_1, finger_table(x)_4, finger_table(x)_5)
  }

  def contains3(hid:BigInt,n:BigInt,n_suc:BigInt):Boolean = {
    if (n < n_suc){
      return (n <= hid && hid < n_suc)
    } else {
      if( n_suc != 0){
        return (n <= hid && hid < BigInt(2).pow(max_bits)) || (0 <= hid && hid < n_suc)
      }else{
        return (n <= hid && hid < BigInt(2).pow(max_bits))
      }

    }
  }

  def sendMsg() = {
    val url = "http://whatthecommit.com/index.txt"
    val result = scala.io.Source.fromURL(url).mkString
    val m_dest_hash:String = md.digest(result.toString.getBytes("UTF-8")).map("%02x".format(_)).mkString
    //println("new data: "+result)
    val m_dest_id:BigInt = m_dest_hash.toList.map( "0123456789abcdef".indexOf(_)).map( BigInt(_)).reduceLeft( _ * 16 + _)

    var i = 0
    while( i<max_bits && !contains3(m_dest_id,finger_table(i)._2, finger_table(i)._3)){
      i += 1
    }
    if (i >= max_bits){
      println("something wrong, exiting...")
      System.exit(-1)
    }
    // if(hash_id == m_dest_id){
    //   var str = m_dest_hash+":"+result
    //   data = str::data
    //   println("stored:"+str)
    // }else{
    //   finger_table(i)._5 ! Message(m_dest_id,m_dest_hash+":"+result)
    // }
    //println(result)
    self ! Message(0, m_dest_id,m_dest_hash+":"+result)
  }

  def fwd_and_store(jump:Int,dest_id:BigInt,str:String) = {
    if(debug){
      println(hash_id+" is forwarding a message to "+dest_id)
    }
    if(hash_id == dest_id){
      data = str::data
      master ! jump
      println("location:data = "+str)
    }else{
      var i = 0
      while( i<max_bits && !contains3(dest_id,finger_table(i)._2, finger_table(i)._3)){
        i += 1
      }
      if (i >= max_bits){
        println("something wrong, exiting...")
        System.exit(-1)
      }
      //[start  interval_start  interval_end  first_node   ref]
      if(finger_table(i)._4 > finger_table(i)._3 || finger_table(i)._4 <= finger_table(i)._2 //first node of interval is out of interval
      || finger_table(i)._4 >= dest_id //first node of interval > dest
      || (finger_table(i)._2 > finger_table(i)._3 && dest_id >= finger_table(i)._2 && finger_table(i)._4 < finger_table(i)._3) )//cross zero point
      {
        if(debug){
          println("dest change from "+dest_id+" to "+finger_table(i)._4)
        }
        finger_table(i)._5 ! Message(jump+1,finger_table(i)._4,str)
      }else{
        finger_table(i)._5 ! Message(jump+1,dest_id,str)
      }
    }
  }


  //println("successor of "+hash_id+"("+int_id+") is "+finger_table(0)._4)
  //println("=========================")
  ini = true



  def receive = {
    case "successor" => sender ! (finger_table(0)._5, finger_table(0)._4)
    case "predecessor" => sender ! (pre_ref, pre_id)
    case "ini" => sender ! ini
    case "hash_id" => sender ! hash_id
    case "sendMsg" => sendMsg()
    case Message(jump:Int,dest_id:BigInt,str:String) => fwd_and_store(jump,dest_id,str)
    case Finger(x) => sender ! (finger_table(x)._5, finger_table(x)._4)
    case U_Finger(x, ref, hash) => sender ! u_finger(x,ref,hash)
    case U_Predecessor(ref, hash) => sender ! u_pre(ref,hash)
    case Finger_s_n_r(x:Int) => sender ! get_finger_s_n_r(x)
    case x => println("does not match any patterns,"+ x);
  }

}

class Master() extends Actor {
  val debug = false //the other debug flag is in line 30
  var total_jump = 0
  var total_feedback = 0
  var req = -1
  val max_bits = 160
  var given_nodes = -1
  implicit val timeout = Timeout(10 seconds)
  import Master._
  import context.dispatcher
  var nodes_number = 0
  var mutex_lock = false

  var node_list: List[ActorRef] = List()

  def add() = {
    if(mutex_lock == false){
      mutex_lock = true
      //println("master: creating node "+nodes_number)
      val n_ref = context.system.actorOf(Props(new Node(nodes_number,self)))
      //println(nodes_number+" at "+n_ref)
      var future = n_ref ? "ini"
      var ini = Await.result(future, Duration.Inf).asInstanceOf[Boolean]
      while (!ini){
        Thread.sleep(100)
        future = n_ref ? "ini"
        ini = Await.result(future, Duration.Inf).asInstanceOf[Boolean]
      }
      var n_id:BigInt = 0
      if(nodes_number >0){
        val any_ref = any_node()
        future = any_ref ? "hash_id"
        var any_id = Await.result(future, Duration.Inf).asInstanceOf[BigInt]

        future = n_ref ? "hash_id"
        n_id = Await.result(future, Duration.Inf).asInstanceOf[BigInt]
        //val (suc_ref,suc_hash) = find_successor(any_ref,any_id,n_id)
        //println("master: successor of "+n_id+" is "+suc_hash)
        //println("master: joining node "+n_id)
        join(n_ref,n_id,any_ref,any_id)

        //then set suc_ref and suc_hash in n_ref
      }
      node_list = node_list :+ n_ref
      nodes_number += 1
      mutex_lock = false

      // if(nodes_number >= 2){
      //   for (node <- node_list){
      //     var future = node ? "hash_id"
      //     var me = Await.result(future, Duration.Inf).asInstanceOf[BigInt]
      //     future = node ? Finger(0)
      //     var (next_ref,next_id) = Await.result(future, Duration.Inf).asInstanceOf[(ActorRef,BigInt)]
      //     future = node ? "predecessor"
      //     var (pre_ref,pre_id) = Await.result(future, Duration.Inf).asInstanceOf[(ActorRef,BigInt)]
      //     println(me+"-next->"+next_id)
      //     println(me+"-pre->"+pre_id)
      //   }
      // }

      if(n_id != 0){
        //println("joining "+n_id)
      }
      println(nodes_number-1 + " joined")
      if (req >0 && given_nodes >0 && nodes_number == given_nodes){
        for (i <- 0 until req){
          Thread.sleep(1000)
          //println("length:"+node_list.length)
          for(n <- node_list){
            n ! "sendMsg"
          }
          Thread.sleep(1000)
        }
      }
    }else{
      context.system.scheduler.scheduleOnce(100 millis, self, "add")
    }

  }

  def send() = {
    for(n <- node_list){
      n ! "sendMsg"
    }
  }


  // if hid in  (n,n_suc]
  def contains(hid:BigInt,n:BigInt,n_suc:BigInt):Boolean = {
    if (n < n_suc){
      return (n < hid && hid<= n_suc)
    } else {
      return (n < hid && hid < BigInt(2).pow(max_bits)) || (0 <= hid && hid <= n_suc)
    }
  }

  // if hid in  (n,n_suc)
  def contains2(hid:BigInt,n:BigInt,n_suc:BigInt):Boolean = {
    if (n < n_suc){
      return (n < hid && hid < n_suc)
    } else {
      if( n_suc != 0){
        return (n < hid && hid < BigInt(2).pow(max_bits)) || (0 <= hid && hid < n_suc)
      }else{
        return (n < hid && hid < BigInt(2).pow(max_bits))
      }

    }
  }

  // if hid in  [n,n_suc)
  def contains3(hid:BigInt,n:BigInt,n_suc:BigInt):Boolean = {
    if (n < n_suc){
      return (n <= hid && hid < n_suc)
    } else {
      if( n_suc != 0){
        return (n <= hid && hid < BigInt(2).pow(max_bits)) || (0 <= hid && hid < n_suc)
      }else{
        return (n <= hid && hid < BigInt(2).pow(max_bits))
      }

    }
  }


  def any_node():ActorRef = {
    val r = scala.util.Random
    var rdm = r.nextInt(nodes_number)
    return node_list(rdm)
  }

  def find_successor(n_ref:ActorRef, n_id:BigInt, id:BigInt):(ActorRef,BigInt) = {
    if(debug){
      println("find_successor")
    }
    // np means n prime
    val (np_ref, np_id)= find_predecessor(n_ref,n_id,id)
    val f = np_ref ? "successor"
    val (np_suc_ref, np_suc_id) = Await.result(f, Duration.Inf).asInstanceOf[(ActorRef,BigInt)]
    return (np_suc_ref, np_suc_id)
  }

  def find_predecessor(n_ref:ActorRef, n_id:BigInt, id:BigInt):(ActorRef,BigInt) = {
    if(debug){
      println("find_predecessor")
    }
    var np_id = n_id
    var np_ref = n_ref
    var f = np_ref ? "successor"
    var (np_suc_ref,np_suc_id) = Await.result(f,Duration.Inf).asInstanceOf[(ActorRef,BigInt)]
    while (!contains(id, np_id, np_suc_id) ){
      if(debug){
        println("call closest_preceding_finger",id,np_id,np_suc_id)
      }
      var t = closest_preceding_finger(np_ref, np_id, id)
      np_ref = t._1
      np_id = t._2

      f = np_ref ? "successor"
      var t2= Await.result(f,Duration.Inf).asInstanceOf[(ActorRef,BigInt)]
      np_suc_ref = t2._1
      np_suc_id = t2._2
    }
    return (np_ref,np_id)
  }

  def closest_preceding_finger(n_ref:ActorRef, n_id:BigInt, id:BigInt):(ActorRef,BigInt) = {
    if(debug){
      println("closest_preceding_finger")
    }
    for(i <- 1 to max_bits){
      var t = max_bits - i
      var f = n_ref ? Finger(t)
      var (n_finger_ref,n_finger_id) = Await.result(f,Duration.Inf).asInstanceOf[(ActorRef,BigInt)]
      if (contains2(n_finger_id, n_id, id)){
        return (n_finger_ref,n_finger_id)
      }
    }
    return (n_ref,n_id)
  }

  def join(n_ref:ActorRef,n_id:BigInt,np_ref:ActorRef,np_id:BigInt) = {
    init_finger_table(n_ref,n_id,np_ref,np_id)
    update_others(n_ref,n_id)
  }

  def init_finger_table(n_ref:ActorRef,n_id:BigInt,np_ref:ActorRef,np_id:BigInt) = {
    if(debug){
      println("init_finger_table")
    }
    val (suc_ref,suc_hash) = find_successor(np_ref,np_id,n_id)
    var future = n_ref ? U_Finger(0,suc_ref,suc_hash)
    var t = Await.result(future, Duration.Inf).asInstanceOf[Boolean]

    future = suc_ref ? "predecessor"
    var (suc_pre_ref,suc_pre_id) = Await.result(future, Duration.Inf).asInstanceOf[(ActorRef,BigInt)]

    future = n_ref ? U_Predecessor(suc_pre_ref,suc_pre_id)
    t = Await.result(future, Duration.Inf).asInstanceOf[Boolean]

    future = suc_ref ? U_Predecessor(n_ref,n_id)
    t = Await.result(future, Duration.Inf).asInstanceOf[Boolean]

    for(i<- 1 until max_bits){
      future = n_ref ? Finger_s_n_r(i)
      var (i_start, i_node_id, i_node_ref) = Await.result(future, Duration.Inf).asInstanceOf[(BigInt,BigInt, ActorRef)]
      future = n_ref ? Finger_s_n_r(i-1)
      var (im1_start, im1_node_id, im1_node_ref) = Await.result(future, Duration.Inf).asInstanceOf[(BigInt,BigInt, ActorRef)]

      if(contains3(i_start, n_id, im1_node_id)){
        future = n_ref ? U_Finger(i,im1_node_ref,im1_node_id)
        t = Await.result(future, Duration.Inf).asInstanceOf[Boolean]
      }else{
        val (next_ref, next_id) = find_successor(np_ref,np_id,i_start)
        future = n_ref ? U_Finger(i,next_ref,next_id)
        t = Await.result(future, Duration.Inf).asInstanceOf[Boolean]
      }
    }

  }

  def update_others(n_ref:ActorRef,n_id:BigInt) = {
    if(debug){
      println("update_others")
    }
    for (i <- 0 until max_bits){
      var pre_id = n_id-BigInt(2).pow(i)
      if(pre_id < 0){
        pre_id = BigInt(2).pow(max_bits) + pre_id
      }
      var (p_ref, p_id) = find_predecessor(n_ref,n_id,pre_id)
      update_finger_table(p_ref, p_id,n_ref ,n_id, i)
    }
  }

  def update_finger_table(n_ref:ActorRef,n_id:BigInt,s_ref:ActorRef, s_id:BigInt,i:Int):Unit = {
    if(debug){
      println("update_finger_table")
    }
    var future = n_ref ? Finger(i)
    var (n_fi_ref,n_fi_id) = Await.result(future, Duration.Inf).asInstanceOf[(ActorRef,BigInt)]
    if( contains2(s_id, n_id, n_fi_id) ){
      if(debug){
        println(s_id,n_id,n_fi_id)
      }
      var future = n_ref ? U_Finger(i,s_ref,s_id)
      var t = Await.result(future, Duration.Inf).asInstanceOf[Boolean]
      future = n_ref ? "predecessor"
      var (n_p_ref,n_p_id) = Await.result(future, Duration.Inf).asInstanceOf[(ActorRef,BigInt)]
      update_finger_table(n_p_ref,n_p_id,s_ref,s_id,i)
    }else{
      if(debug){
        println("out",s_id,n_id,n_fi_id)
      }
    }
  }

  def jump_add(jump:Int) = {
    total_jump += jump
    total_feedback += 1
    if(total_feedback == req*given_nodes){
      println("")
      println("Total number of jumps:"+total_jump)
      println("Total number of request:"+total_feedback)
      println("Average number of hops:"+total_jump.toDouble/total_feedback.toDouble)
      println("")
      System.exit(0)
    }
  }


  def receive = {
    case Request_num(x) => req = x
    case Node_num(x) => given_nodes = x
    //case Find_successor(ref,id,dest_id) => sender ! find_successor(ref,id,dest_id)
    case "add" => add()
    case "any_node" => sender ! any_node()
    case "send" => send()
    case jump:Int => jump_add(jump)
    case _ =>
  }

}

object Main {

  def main(args: Array[String]) = {
    import Master._

    val nodes = args(0).toInt
    val requests = args(1).toInt
    val system = ActorSystem("proj3")
    val master = system.actorOf(Props[Master])
    master ! Request_num(requests)
    master ! Node_num(nodes)
    for (i <- 0 until nodes){
      master ! "add"
    }

    // Thread.sleep(5000)
    // print("start send message? (y/n) :")
    // val ln = readLine()
    // if (ln != null){
    //   println(ln)
    //   if(ln == "y" || ln == "yes"){
    //     master ! "send"
    //   }
    // }

    //System.exit(0)
  }
}
