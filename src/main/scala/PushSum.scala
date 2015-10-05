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
 import scala.concurrent.duration._
  import akka.util.Timeout
 import scala.concurrent.ExecutionContext.Implicits.global
 import scala.util.Success
import scala.util.Failure
import akka.pattern.{ after, ask, pipe }
import scala.concurrent.Future
 
 case object CreatePushSumWorkers
 case object StartPushSum
 case object StartPushSumImp
 case object Algo_PS
 case object AlgoPSPercent
 case class Message(tS : Double, tW : Double)
 case class MessageImp(tS : Double, tW : Double)
 case object StopAll
 case object StopMe
 case object StoppedWorker
 case class FailPS(failCount : Int)

 
object PushSum {
     
     def algo_PS(N : Int, top : Map[Int, List[Int]]) = {
         
         val system = ActorSystem("PushSum")
         val master = system.actorOf(Props(new PushSumMaster(N, top, System.currentTimeMillis)), "pushsummaster")
         master ! CreatePushSumWorkers
         master ! Algo_PS
         println("Gossip.algo_PS ENDS")
         
     }
     
     def algo_PS(N : Int, top : Map[Int, List[Int]], percent : Int) = {
         
         val system = ActorSystem("PushSum")
         val master = system.actorOf(Props(new PushSumMaster(N, top, System.currentTimeMillis)), "pushsummaster")
         master ! CreatePushSumWorkers
         master ! FailPS(N*percent/100)
         master ! AlgoPSPercent
         println("Gossip.algo_PS ENDS")
     }
     
 }
 
 class PushSumMaster(N : Int, top : Map[Int, List[Int]], startTime: Long) extends Actor {
     
     var workerPool = new Array[ActorRef](N+1)
     var stopcount = 0
     var terminate = false
     val system = ActorSystem("PushSum")
     var failCount = 0
     val startBuf = scala.collection.mutable.ListBuffer.empty[Int]
     
     def receive = {
         case CreatePushSumWorkers => createPushSumWorkers()
         case Algo_PS =>    var startIdx = Random.nextInt(N) + 1
                            workerPool(startIdx) ! StartPushSum
                            
         case AlgoPSPercent =>  var startIdx = Random.nextInt(N) + 1
                                println("Staring Idx: " + startIdx)
                                workerPool(startIdx) ! StartPushSumImp
                                startBuf += startIdx
                                failWorkers()
                                
         case FailPS(failcount) => failCount = failcount
         case StopAll => //println("STOPPING ALL " + terminate)
                         terminate = true
                         stopAll()    
         case StoppedWorker => stopcount += 1
                               if (stopcount == N){
                                   stopMaster()
                                   /*context.system.shutdown()
                                   println("PUSHSUM Time: " + (System.currentTimeMillis - startTime))*/
                               }
         case default => println("PushSumMaster DEFAULT")
     }
     
     def failWorkers() {
           var f = 0
           val failBuf = scala.collection.mutable.ListBuffer.empty[Int]
           println("FailCount: "+failCount)
           for (f <- 1 to failCount){
               var failIdx = Random.nextInt(N) + 1
               println("CHECK COLLISION: " + failIdx)
               while (failBuf.contains(failIdx) || startBuf.contains(failIdx)){
                   failIdx = Random.nextInt(N) + 1
               }
               failBuf += failIdx
               println("Failing Worker: " + failIdx)
               workerPool(failIdx) ! StopMe
           }
     }
     
     def stopAll() = {
         var i = 1
         for(i <- 1 to N) {
             workerPool(i) ! StopMe
         }
         println("STOPPED ALL")
         stopMaster()
     }
     
     def stopMaster() {
        context.system.shutdown()
        println("PUSHSUM Time: " + (System.currentTimeMillis - startTime))
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
     var w : Double = 0
     var q = scala.collection.mutable.Queue[Double]()
     q.enqueue(1000)
     q.enqueue(1000)
     q.enqueue(1000)
     var threshold : Double = math.pow(10, -10)
     
     val fallbackTimeout = 2 seconds
	 implicit val timeout = new Timeout(5 seconds)
	 require(fallbackTimeout < timeout.duration)
	 
	 val failedBuf = scala.collection.mutable.ListBuffer.empty[String]
	 
     
     def receive = {
         case StartPushSum      =>  w = 1
                                    propogate()
         case StartPushSumImp    => w = 1
                                   sendMessage(s, w)
                                    
         case Message(tS, tW)   => s += tS
                                   w += tW
                                   printState()
                                   if (stopCheck()) {
                                        println("STOP CHECK")
                                        context.parent ! StopAll
                                   } else{
                                       propogate()
                                   }
         case MessageImp(tS, tW) => s += tS
                                   w += tW
                                   printState()
                                   if (stopCheck()) {
                                        println("STOP CHECK")
                                        context.parent ! StopAll
                                   } else{
                                       sendMessage(s,w)
                                   }
         
         case StopMe => stopMe()
         case default => println("PushSumWorker DEFAULT")
     }
     
     def sendMessage(tS : Double, tW : Double) {
         implicit val timeout = FiniteDuration(1,"seconds")
         var neis = getNeis(top, workerId) // get Neis of curr in topology
         
         //println("Resolve One")
         //println(context.self.path)
         //println(context.parent.path)
         
         if (failedBuf.size == neis.size){
             println("All Neighbors FAILED - CHECK !")
             stopMe()
         }
         
         var randNei = getRandomNei(neis)
         while (failedBuf.contains(randNei)) {
             randNei = getRandomNei(neis)
         }
         var randWorker = "Worker" + randNei
         
         context.actorSelection(context.parent.path+"/" + randWorker).resolveOne(timeout).onComplete {

                case Success(actor) => 
                  s = s/2
                  w = w/2
                  //println("Success in Resolving " + workerId)
                  actor ! MessageImp(s, w);
                  
                case Failure(ex) =>
                  println("Failure in Resolving "+ workerId)
                  //connectedNodes = connectedNodes.filter (_ != connectedNodes(indexToSend))
                  sendMessage(tS,tW)

        }

     }
     
     def stopMe() {
         //println("STOP ME " + workername)
         context.parent ! StoppedWorker
         context.stop(self)
     }
     
     def propogateImp() {
        println("INSIDE PROPOGATE IMP" + workerId)
        var neis = getNeis(top, workerId) // get Neis of curr in topology
        var randNei = getRandomNei(neis)
        println("Random Nei: "+ randNei)
        //workerPool(randNei) ? MessageImp(s/2, w/2)
       val future = (workerPool(randNei) ? MessageImp(s/2, w/2))
                     .recover {
                         case default =>    println("RECOVER")
                                            propogateImp()
                     }
                     .onComplete{
                         case Success(results) => //println("SUCESSFUL FUTURE Inside recurse()")
                                                  s = s/2
                                                  w = w/2
                                                  printState()
                                                  println("FUTURE SUCCESS - REDUCE VALUES")
                         case Failure(t) => println("FAILURE DETECTED Worker: " + randNei)
                                            propogateImp()
                     }
     }
     
     def propogate() {
        var neis = getNeis(top, workerId) // get Neis of curr in topology
        var randNei = getRandomNei(neis)
        s = s/2
        w = w/2
        printState()
        workerPool(randNei) ! Message(s, w)
     }
     
     def printState() {
         println(workername + " - s: " + s + " w: " + w + " s/w: " + s/w)
     }
     
     def stopCheck() : Boolean = {
        q.dequeue()
         q.enqueue(s/w)
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
         //println("DIFF: " + a + " " + b + " :: "+ Some(math.abs(cd)))
         Some(math.abs(cd))
     }
     
     def getNeis(top : Map[Int, List[Int]], curr : Int) : List[Int] = {
         return top(curr)

     }
     
     def getRandomNei(neis : List[Int]) : Int= {
         return neis(Random.nextInt(neis.length))
     }
 }