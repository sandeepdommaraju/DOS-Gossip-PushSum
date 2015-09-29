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
 
 case object CreateGossipWorkers
 case object Algo
 case class Rumor(rumor: String)
 case object StoppedRumor
 case object Stop
 case object StopAck
 
 object Gossip {
     
     def algo_G(N : Int, C : Int, R : Int, top : Map[Int, List[Int]]) = {
         
         val system = ActorSystem("Gossip")
         val master = system.actorOf(Props(new GossipMaster(N, C, R, top)), "gossipmaster")
         master ! CreateGossipWorkers
         master ! Algo
         println("Gossip.algo_G ENDS")
         
     }
     
 }
 
 class GossipMaster(N : Int, C : Int, R : Int, top : Map[Int, List[Int]]) extends Actor {
     
     var workerPool = new Array[ActorRef](N+1)
     var rumors = List.range(1, R + 1).map( x => "Rumor" + x)
     var stoppedRumors = 0
     var stopAckCount = 0
     
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
         case StoppedRumor => stoppedRumors += 1
                                println("stoppedRumors: " + stoppedRumors)
                                if (stoppedRumors == R) {
                                    stopAllWorkers()
                                }
         case StopAck => stopAckCount += 1
                         if (stopAckCount == N){
                             println("SHUT DOWN")
                             context.system.shutdown()
                         }
         case default => println("GossipMaster - DEFAULT")
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
         case Stop =>    println("Stopping " + self.path.name)
                         context.parent ! StopAck
                         context.stop(self)
         case default => println("GossipWorker - DEFAULT")
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
     
     /*def terminateWorker() : Boolean = {
         var terminate = true
         var count = 0
         rumorMap.foreach {
             tuple => count += 1
                      terminate = terminate && terminationCheck(tuple._1)
         }
         return terminate && (count == R)
     }*/
     
 }
 
 
      /*def algo_G_old(N : Int, C : Int, R : Int, top : Map[Int, List[Int]]) = {
     
         
         var rumors = List.range(1, R + 1).map( x => "Rumor" + x)
         
         var nodeMap = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[String, Int]]() // <nodeNum , <Rumor, rumorCount>>
         
         var r = 0
         
         var debug = true
         
         while (r < rumors.length){
             var startIdx = Random.nextInt(N) + 1
             
             if (debug) {
                 println("Starting Node: "+startIdx)
             }
             
             //sleep for some time
             if (debug) {
                 println("SLEEP and putInMap")
             }
             
             // TODO send to startIdx Worker 
             putInMap(nodeMap, startIdx, rumors(r))
             
             if (debug) {
                 println("StartGossip for " + rumors(r))
             }
             
             startGossips(top, nodeMap, startIdx, rumors(r), C)
             
             r += 1
             
         }
         
         if (debug) {
             println("END GossipAlgo")
         }
     }
     
     def startGossips(top : Map[Int, List[Int]], nodeMap : scala.collection.mutable.Map[Int, scala.collection.mutable.Map[String, Int]], startIdx : Int, rumor : String, C : Int) = {
         
         // run this logic in new GossipMaster
         
         var debug = true;
         
         var curr = startIdx
         
         var loopCount = 0;
         
         breakable { while (loopCount < 1000 && nodeMap.size >= 0) {
         
             
             var neis = getNeis(top, curr) // get Neis of curr in topology
             
             var randNei = getRandomNei(neis)
             
             if (debug) {
                 println(curr + " -RandNei- " + randNei)
             }
             
             if (debug) {
                 println("startGossips putInMap")
             }
             
             // TODO send to randNei Worker 
             putInMap(nodeMap, randNei, rumor)
             
             //stop randNei Worker if has received rumor C times
             if (terminationCheck(nodeMap, randNei, rumor, C)) {
                 //terminate Worker
                 nodeMap -= randNei
                 println("Terminate: " + randNei)
                 
                 if (nodeMap.size == 0) {
                     break
                 }
             }
             
             curr = randNei
             
             loopCount += 1
             
             println("LOOP: " + loopCount)
             
         }
         }
         
         println("LoopCount: " + loopCount)
         
         
     }
     
     def terminationCheck(nodeMap : scala.collection.mutable.Map[Int, scala.collection.mutable.Map[String, Int]], idx : Int, rumor : String, C : Int) : Boolean= {
         println("terminateCheck: " + nodeMap(idx)(rumor))
         return nodeMap(idx)(rumor) == C
     }
     
     def getNeis(top : Map[Int, List[Int]], curr : Int) : List[Int] = {
         return top(curr)

     }
     
     def getRandomNei(neis : List[Int]) : Int= {
         return neis(Random.nextInt(neis.length))
     }
     
     def putInMap(nodeMap : scala.collection.mutable.Map[Int, scala.collection.mutable.Map[String, Int]], key : Int, rumor : String) = {
         nodeMap.get(key) match {
            case Some(rumorMap : collection.mutable.Map[String, Int]) => rumorMap.get(rumor) match {
                                                                            case Some(count : Int)  => rumorMap.update(rumor , count + 1)
                                                                            case None               => rumorMap += (rumor -> 1)
                                                                         }
            case None   => nodeMap += (key -> collection.mutable.Map(rumor -> 1))
         }
         println(nodeMap)
     }*/