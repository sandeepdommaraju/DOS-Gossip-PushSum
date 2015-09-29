/**
 *  1. Each actor has s, w ; Initially s = i and w = 1
 *  2. Start one actor at random
 *  3. Each message is a pair (s, w)
 *  4. On receive this.s += s; this.w += w
 *  5. update current values to half (this.s/2, this.w/2) , send half values (this.s/2, this.w/2)
 *  6. At any time sum estimate is (this.s/this.w)
 *  7. If an actors (s/w) ratio did not change more than 10^-10 in 3 consecutive rounds, terminate the actor
 *
 **/
 
 /**
  * DOUBT: How to implement heartbeat or clock of the system
  **/ 



 import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox }
 import util.Random
 import scala.collection
 import scala.util.control.Breaks._
 import scala.math.BigDecimal
 
 case object CreatePushSumWorkers
 case object StartPushSum
 case object Algo_PS
 case class Message(tS : Double, tW : Double)
 
object PushSum {
     
     def algo_PS(N : Int, top : Map[Int, List[Int]]) = {
         
         val system = ActorSystem("PushSum")
         val master = system.actorOf(Props(new PushSumMaster(N, top)), "pushsummaster")
         master ! CreatePushSumWorkers
         master ! Algo_PS
         println("Gossip.algo_PS ENDS")
         
     }
     
 }
 
 class PushSumMaster(N : Int, top : Map[Int, List[Int]]) extends Actor {
     
     var workerPool = new Array[ActorRef](N+1)
     
     def receive = {
         case CreatePushSumWorkers => createPushSumWorkers()
         case Algo_PS =>    var startIdx = Random.nextInt(N) + 1
                            workerPool(startIdx) ! StartPushSum
                            println("todo")
         case default => println("PushSumMaster DEFAULT")
     }
     
     def createPushSumWorkers() {
        var i = 1
        for (i <- 1 to N){
            workerPool(i) = context.actorOf(Props(new PushSumWorker(N, top, workerPool)), name="Worker"+i)
        }
        println("CREATE WORKERS")
     }
 }
 
 class PushSumWorker(N : Int, top : Map[Int, List[Int]], workerPool : Array[ActorRef]) extends Actor {
     
     var workername = self.path.name
     var workerId   = Integer.parseInt(workername.substring(6))
     var s : Double = workerId
     var w : Double = 1
     var q = scala.collection.mutable.Queue[Double]()
     q.enqueue(1000)
     q.enqueue(1000)
     q.enqueue(1000)
     var threshold : Double = math.pow(10, -10)
     
     def receive = {
         case StartPushSum      =>  propogate()
                                    
         case Message(tS, tW)   => s += tS
                                   w += tW
                                   if (stopCheck()) {
                                        println("STOP ME")
                                        stopMe()
                                   } else{
                                       propogate()
                                   }
         
         case default => println("PushSumWorker DEFAULT")
     }
     
     def stopMe() {
         context.stop(self)
     }
     
     def propogate() {
        printState()
        var neis = getNeis(top, workerId) // get Neis of curr in topology
        var randNei = getRandomNei(neis)
        s = s/2
        w = w/2
        workerPool(randNei) ! Message(s, w)
     }
     
     def printState() {
         println(workername + " - s: " + s + " w: " + w + " s/w: " + s/w)
     }
     
     def stopCheck() : Boolean = {
         q.dequeue()
         q.enqueue(s/w)
         println("Evaluating stopCheck")
         diff(q.get(0), q.get(1)) match {
             case Some(value1) => if (value1 > threshold) {
                                return false
                            } else {
                                
                                diff(q.get(1), q.get(2)) match {
                                    case Some(value2) => if (value2 > threshold) {
                                        return false
                                    } else {
                                        return true
                                    }
                                    case None => println("NONE MATCH VALUE2")
                                }
                            }
            case None => println("NONE MATCH VALUE1")
         }
         return true
     }
     
     
     def diff(a : Option[Double], b : Option[Double]) : Option[Double] = {
         var ad : Double = a.getOrElse(0) 
         var bd : Double = b.getOrElse(0)
         var cd = ad - bd
         println("DIFF: " + a + " " + b + " :: "+ Some(math.abs(cd)))
         Some(math.abs(cd))
     }
     
     def getNeis(top : Map[Int, List[Int]], curr : Int) : List[Int] = {
         return top(curr)

     }
     
     def getRandomNei(neis : List[Int]) : Int= {
         return neis(Random.nextInt(neis.length))
     }
 }