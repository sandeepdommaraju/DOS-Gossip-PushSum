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
 
 import util.Random
 import scala.collection
 
 object Gossip {
     
     def algo_G() = {
     
         var N = 5 //total nodes
         
         var C = 10 //stop if rumor count = C
         
         var top : Map[Int, List[Int]] = Map() //LoadTopology
         
         var rumors = List("Rumor1", "Rumor2", "Rumor3")
         
         var nodeMap = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[String, Int]]() // <nodeNum , <Rumor, rumorCount>>
         
         var r = 0
         
         while (r < rumors.length){
             var startIdx = Random.nextInt(N) + 1
             
             //sleep for some time
             
             // TODO send to startIdx Worker 
             putInMap(nodeMap, startIdx, rumors(r))
             
             startGossips(top, nodeMap, startIdx, rumors(r), C)
             
         }
     }
     
     def startGossips(top : Map[Int, List[Int]], nodeMap : scala.collection.mutable.Map[Int, scala.collection.mutable.Map[String, Int]], startIdx : Int, rumor : String, C : Int) = {
         
         // run this logic in new GossipMaster
         
         var curr = startIdx
         
         while (true) {
             
             var neis = getNeis(top, curr) // get Neis of curr in topology
             
             var randNei = getRandomNei(neis)
             
             // TODO send to randNei Worker 
             putInMap(nodeMap, randNei, rumor)
             
             //stop randNei Worker if has received rumor C times
             if (terminationCheck(nodeMap, randNei, rumor, C)) {
                 //terminate Worker
             }
             
             curr = randNei
             
         }
         
         
     }
     
     def terminationCheck(nodeMap : scala.collection.mutable.Map[Int, scala.collection.mutable.Map[String, Int]], idx : Int, rumor : String, C : Int) = {
         nodeMap(idx)(rumor) == C
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
     }
     
     /*def getFromMap() = {
         
     }*/
     
 }