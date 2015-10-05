/**
 *  1. One of the actors is told a rumor
 *  2. Each actor selects random neigbor and tells the rumor
 *  3. Each actor keeps track of rumors and how many times it has heard the rumor.
 *  4. It stops transmitting once it has heard the rumor C times
 *
 **/
 
 /**
  * DOUBT: How to implement heartbeat or clock of the system
  **/ 
 
 import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox }
 import util.Random
 import scala.collection
 import scala.util.control.Breaks._
 import scala.concurrent.duration._
 import akka.util.Timeout
 import scala.concurrent.ExecutionContext.Implicits.global
 import scala.util.Success
import scala.util.Failure
import akka.pattern.{ after, ask, pipe }
import scala.concurrent.Future
 
 case object CreateGossipWorkers
 case object Algo
 case object AlgoPercent
 case class Rumor(rumor: String)
 case class RumorImp(rumor: String)
 case object StoppedRumor
 case object Stop
 case object StopAck
 case object TempFail
 case object PermFail
 case class Fail(num : Int)
 
 object Gossip {
     
     def algo_G(N : Int, C : Int, R : Int, top : Map[Int, List[Int]]) = {
         
         val system = ActorSystem("Gossip")
         val master = system.actorOf(Props(new GossipMaster(N, C, R, top, System.currentTimeMillis)), "gossipmaster")
         master ! CreateGossipWorkers
         master ! Algo
         println("Gossip.algo_G ENDS")
         
     }
     
     def algo_G(N : Int, C : Int, R : Int, top : Map[Int, List[Int]], percent : Int) = {
         
         val system = ActorSystem("Gossip")
         val master = system.actorOf(Props(new GossipMaster(N, C, R, top, System.currentTimeMillis)), "gossipmaster")
         master ! CreateGossipWorkers
         master ! AlgoPercent
         master ! Fail(N*percent/100)
         println("Gossip.algo_G ENDS")
         
     }
     
 }
 
 class GossipMaster(N : Int, C : Int, R : Int, top : Map[Int, List[Int]], startTime : Long) extends Actor { 
     
     var workerPool = new Array[ActorRef](N+1)
     var rumors = List.range(1, R + 1).map( x => "Rumor" + x)
     var stoppedRumors = 0
     var stopAckCount = 0
     implicit val timeout = Timeout(1 seconds) // needed for `?` below
     var failCount = 0
     val startBuf = scala.collection.mutable.ListBuffer.empty[Int]
     
     def receive = {
         case CreateGossipWorkers => createGossipWorkers()
         case Algo => var r = 0
                      for (r <- 1 to R) {
                          var rumor = rumors(r-1)
                          var startIdx = Random.nextInt(N) + 1
                          //sleep
                          workerPool(startIdx) ! Rumor(rumor)
                      }
                      println("ALGO ENDS")
         case AlgoPercent => var r = 0
                      while (r < R) {
                          var rumor = rumors(r)
                          var startIdx = Random.nextInt(N) + 1
                          workerPool(startIdx) ! RumorImp(rumor)
                          r += 1
                          startBuf += startIdx
                      }
                      failWorkers()
                      println("ALGO ENDS")
         case Fail(failcount) => failCount = failcount
         case StoppedRumor => stoppedRumors += 1
                                println("stoppedRumors: " + stoppedRumors)
                                if (stoppedRumors == R) {
                                    stopAllWorkers()
                                }
         case StopAck => stopAckCount += 1
                         if (stopAckCount == N){
                             context.system.shutdown()
                             println("Gossip Time: " + (System.currentTimeMillis - startTime))
                         }
         case default => println("GossipMaster - DEFAULT")
     }
     
     def failWorkers() {
           var f = 0
           val failBuf = scala.collection.mutable.ListBuffer.empty[Int]
           for (f <- 1 to failCount){
               var failIdx = Random.nextInt(N) + 1
               println("CHECK COLLISION: " + failIdx)
               while (failBuf.contains(failIdx) || startBuf.contains(failIdx)){
                   failIdx = Random.nextInt(N) + 1
               }
               failBuf += failIdx
               workerPool(failIdx) ! Stop
           }
     }
     
     
     def stopAllWorkers() {
         var i = 1
         for(i <- 1 to N) {
             workerPool(i) ! Stop
         }
     }
     
     def createGossipWorkers() {
        var i = 1
        for (i <- 1 to N){
            workerPool(i) = context.actorOf(Props(new GossipWorker(N, C, R, top, workerPool)), name="Worker"+i)
        }
        println("CREATED WORKERS")
     }
 }
 
 class GossipWorker(N : Int, C: Int, R : Int, top : Map[Int, List[Int]], workerPool : Array[ActorRef]) extends Actor {
     
     var rumorMap   = scala.collection.mutable.Map[String, Int]()
     var workername = self.path.name
     var workerId   = Integer.parseInt(workername.substring(6))
     
	  val fallbackTimeout = 2 seconds
	  implicit val timeout = new Timeout(5 seconds)
	  require(fallbackTimeout < timeout.duration) 
	  
	  val failedBuf = scala.collection.mutable.ListBuffer.empty[String]
     
     def receive = {
         case Rumor(rumor) => putInMap(rumor)
                              if (terminationCheck(rumor)) {
                                  println(self.path.name + " :: " + rumorMap)
                                  println("stop sending " + rumor + " from: " + self.path.name)
                                  context.parent ! StoppedRumor
                              } else {
                                    println(self.path.name + " :: " + rumorMap)
                                    var neis = getNeis(top, workerId) // get Neis of curr in topology
                                    var randNei = getRandomNei(neis)
                                    workerPool(randNei) ! Rumor(rumor)
                              }
                              
         case RumorImp(rumor) => putInMap(rumor)
                                 if (terminationCheck(rumor)) {
                                      println(self.path.name + " :: " + rumorMap)
                                      println("stop sending " + rumor + " from: " + self.path.name)
                                      context.parent ! StoppedRumor
                                } else {
                                    println(self.path.name + " :: " + rumorMap)
                                    sendMessage(rumor)
                                }
         
         
         
         case Stop =>    println("Stopping " + self.path.name)
                         stopMe()
         case default => println("GossipWorker - DEFAULT")
     }
     
     def stopMe() {
         //println("STOP ME " + workername)
         context.parent ! StopAck
         context.stop(self)
     }
     
     def recurse(rumor : String) : Boolean = {
         
        var neis = getNeis(top, workerId) // get Neis of curr in topology
        var randNei = getRandomNei(neis)
        val future = (workerPool(randNei) ? RumorImp(rumor))
                     .recover {
                         case default => //println("RECOVERING")
                                        return recurse(rumor)
                     }
                     .onComplete{
                         case Success(results) => //println("SUCESSFUL FUTURE Inside recurse()")
                                                  return true
                         case Failure(t) => println("FAILURE DETECTED")
                                            return recurse(rumor)
                     }
        return true
     }
     
     def sendMessage(rumor : String) {
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
                  actor ! RumorImp(rumor);
                  
                case Failure(ex) =>
                  println("Failure in Resolving "+ workerId)
                  //connectedNodes = connectedNodes.filter (_ != connectedNodes(indexToSend))
                  sendMessage(rumor)

        }

     }
     
     def putInMap(rumor : String) = {
         rumorMap.get(rumor) match {
                                       case Some(count : Int)  => rumorMap.update(rumor , count + 1)
                                       case None               => rumorMap += (rumor -> 1)
                                   }
     }
     
     def terminationCheck(rumor : String) : Boolean= {
         return rumorMap(rumor) == C
     }
     
     def getNeis(top : Map[Int, List[Int]], curr : Int) : List[Int] = {
         return top(curr)

     }
     
     def getRandomNei(neis : List[Int]) : Int= {
         return neis(Random.nextInt(neis.length))
     }
     
 }