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
import PushSum.algo_PS
 
 object Main {
     
     def main2(args: Array[String]) {
          
          var topologies = List("line", "full", "3D", "imp3D")
          var algorithms  = List("gossip", "push-sum")
          if (args.length < 3  || !topologies.contains(args(1))  || !algorithms.contains(args(2))) {
              println("Please Enter 3 Args ::: NumNodes Topology{full, 3D, line, imp3D} algorithm{gossip, push-sum}")
              return
          }
          
          var topology = args(1)
          var algorithm = args(2)
          
          //var topology = "full" //"3D" //"full" //line"
          //var algorithm = "gossip"
          //var algorithm = "push-sum"
          
          var N = Integer.parseInt(args(0)) //top.keys.size
          
          println("start creating topology")
          var top = createTopology(topology, N)
          println("stop creating topology")
          
          N = top.keys.size
          
          var C = 3
          
          var R = 3
          
          
          algorithm match {
              case "gossip"     => algo_G(N, C, R, top)
              case "push-sum"   => algo_PS(N, top)
              case default      => println("Default")
          }
          
          println("LAST LINE")
     }
     
 }
 