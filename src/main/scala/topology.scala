/**
 *  creates topologies
 *  1. Line
 *  2. Full Network
 *  3. 3D Grid
 *  4. Imperfect 3D Grid
 * 
 *  For each topology return Map<Integer, List<Integer>> <node, neighbors>
 **/
 
 import util.Random
 
 object Topology extends App{
     
    
    createTopology("line", 4)
    createTopology("full", 4)
    createTopology("3D", 9)
    createTopology("imp3D", 9)
    
    
    def createTopology(topology : String, N : Int) = {
        
        var top : Map[Int, List[Int]] = Map()
        
        topology match {
            case "line" => top = createLine(N)
            case "full" => top = createFull(N)
            case "3D" => top = create3D(N)
            case "imp3D" => top = createImp3D(N)
            case _ => println("ERR: Cant create topology!!")
        }
        
        printTopology(top)
     
    }
    
    def createLine(N : Int) : Map[Int, List[Int]] = {
        println("creating Line Topology with " + N + " nodes !!")
        var top : Map[Int, List[Int]] = Map()
        var n = 1
        for (n <- 1 to N) {
            var neis : List[Int] = List()
            if (isWithinBoundary(n-1, N, List())) neis = neis ::: List(n-1)
            if (isWithinBoundary(n+1, N, List())) neis = neis ::: List(n+1)
            top += (n -> neis)
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
        var neis : List[Int] = List()
        var i = 1
        for (i <- 1 to N) {
            if (isWithinBoundary(i, N, List(n))) neis = neis ::: List(i)
        }
        return neis
    }
    
    def create3D(N : Int) : Map[Int, List[Int]] = {
        println("creating 3D Topology with " + N + " nodes !!")
        var total : Int = N
        if (N%4 != 0) {
            total = N + 4 - N%4
            println("Adding "+ (4 - N%4) + " extra elements to make it square!!")
        }
        var top : Map[Int, List[Int]] = Map()
        
        var n = 1
        for (n <- 1 to total) {
            top += (n -> get3DNeis(n, total))
        }
        return top
    }
    
    def get3DNeis(n : Int, N : Int) : List[Int] = {
        var neis : List[Int] = List()
        
        if (isWithinBoundary(n-4, N, List())) neis = neis ::: List(n-4)  //elem in prev square
        if (isWithinBoundary(n+4, N, List())) neis = neis ::: List(n+4)  //elem in next square
        
        if (n%4 == 0) {
            if (isWithinBoundary(n-3, N, List())) neis = neis ::: List(n-3)
        }
        else if (isWithinBoundary(n+1, N, List())) neis = neis ::: List(n+1) //next elem in same square
        
        if (n%4 == 1) {
            if (isWithinBoundary(n+3, N, List())) neis = neis ::: List(n+3)
        }
        else if (isWithinBoundary(n-1, N, List())) neis = neis ::: List(n-1) //prev elem in same square
        
        return neis
    }
    
    //Nodes should be from 1 to N
    def isWithinBoundary(n : Int, N : Int, exclude : List[Int]) : Boolean = {
        return n >= 1 && n <= N && ((Nil eq exclude) || !exclude.contains(n))
    }
    
    def createImp3D(N : Int) : Map[Int, List[Int]] = {
        println("creating Imperfect-3D Topology with " + N + " nodes !!")
        var top : Map[Int, List[Int]] = Map()
        top = create3D(N);
        top = insertImperfection(top, N)
        return top
    }
    
    def insertImperfection(top : Map[Int, List[Int]], N : Int) : Map[Int, List[Int]] = {
        var newtop : Map[Int, List[Int]] = Map()
        top.foreach {
            
            tuple => var neis : List[Int] = tuple._2
                     var randNei : Int = getRand(N, neis ::: List(tuple._1))
                     neis = neis ::: List(randNei)
                     newtop += (tuple._1 -> neis)
        }
        return newtop
    }
    
    def getRand(N : Int, existingNeis : List[Int]) : Int = {
        var rand : Int = 1
        rand = Random.nextInt(N) + 1
        while ( existingNeis.contains(rand)) {
            rand = Random.nextInt(N) + 1
        }
        return rand
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
 
 