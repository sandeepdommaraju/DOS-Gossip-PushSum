/**
 * Read input: numNodes topology algorithm
 * 1. create topology
 * 2. create workers
 * 3. invoke algorithm
 * 4. Results
 **/

import akka.actor.{ ActorRef, ActorSystem, Props, Actor, Inbox }
import Topology.createTopology
import Gossip.algo_G
 
 object Main {
     
     def main(args: Array[String]) {
          
          var topologies = List("line", "full", "3D", "imp3D")
          var algorithms  = List("gossip", "push-sum")
          /*if (args.length < 3  || !topologies.contains(args(1))  || !algorithms.contains(args(2))) {
              println("Please Enter 3 Args ::: NumNodes Topology{full, 3D, line, imp3D} algorithm{gossip, push-sum}")
              return
          }
          
          var topology = args(1)
          var algorithm = args(2)*/
          
          var topology = "imp3D" //"3D" //"full" //line"
          var algorithm = "gossip"
          
          println("start creating topology")
          var top = createTopology(topology, 5)
          println("stop creating topology")
          
          var N = top.keys.size
          
          var C = 3
          
          var R = 3
          
          algorithm match {
              case "gossip" => algo_G(N, C, R, top)
              case default  => println("Default")
          }
          
          println("LAST LINE")
     }
     
 }
 