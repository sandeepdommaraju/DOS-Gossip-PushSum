/**
 *  creates topologies
 *  1. Line
 *  2. Full Network
 *  3. 3D Grid
 *  4. Imperfect 3D Grid
 * 
 *  For each topology return Map<Integer, List<Integer>> <node, neighbors>
 **/
 
 object Topology extends App{
     
    
    //createTopology("line", 4)
    //createTopology("full", 4)
    
    create3D(10)
    create3D(5)
    create3D(8)
    
    def createTopology(topology : String, N : Int) = {
        
        var top : Map[Int, List[Int]] = Map()
        
        topology match {
            case "line" => top = createLine(N)
            case "full" => top = createFull(N)
            case "3D" => println("3D 3D 3D")
            case "imp3D" => println("IMPERFECT 3D")
            case _ => println("ERR: Cant create topology!!")
        }
        
        printTopology(top)
     
    }
    
    def createLine(N : Int) : Map[Int, List[Int]] = {
        println("creating Line Topology with " + N + " nodes !!")
        var top : Map[Int, List[Int]] = Map()
        var n = 1
        for (n <- 1 to N) {
            if (n == 1) top += (1 -> List(2))
            else if (n == N) top += (n -> List(n-1))
            else top += (n -> List(n-1, n+1))
        }
        
        return top
    }
    
    def createFull(N : Int) : Map[Int, List[Int]] = {
        println("creating Full Network Topology with " + N + " nodes !!")
        var top : Map[Int, List[Int]] = Map()
        
        var n = 1
        for (n <- 1 to N) {
            top += (n -> getFullNeis(n, N))
        }
        
        return top
    }
    
    def getFullNeis(n : Int, N: Int) : List[Int] = {
        //println("get neis for: " + n)
        var neis : List[Int] = List()
        var i = 1
        for (i <- 1 to N) {
            if (i != n) {
                neis = neis ::: List(i)
            }
        }
        //println("neis: " + neis)
        return neis
    }
    
    def create3D(N : Int) : Map[Int, List[Int]] = {
        println("creating 3D Topology with " + N + " nodes !!")
        var total : Int = N
        if (N%4 != 0) {
            total = N + 4 - N%4
        }
        println(total)
        var top : Map[Int, List[Int]] = Map()
        
        var n = 1
        for (n <- 1 to N) {
            top += (n -> get3DNeis(n, total))
        }
        
        return top
    }
    
    def get3DNeis(n : Int, N : Int) : List[Int] = {
        var neis : List[Int] = List()
        return neis
    }
    
    def createImp3D(N : Int) : Map[Int, List[Int]] = {
        println("creating Imperfect-3D Topology with " + N + " nodes !!")
        var top : Map[Int, List[Int]] = Map()
        return top
    }
    
    def printTopology(top : Map[Int, List[Int]]) = {
        println("nodes: " + top.keys)
        var N = top.keys.size
        var n = 1
        for (n <- 1 to N) {
            println("neis of " + n + " : " + top(n))
        }
    }
     
 }
 
 