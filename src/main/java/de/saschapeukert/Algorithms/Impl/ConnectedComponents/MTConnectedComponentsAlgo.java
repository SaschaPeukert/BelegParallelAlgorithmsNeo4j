package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search.MyBFS;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */


@SuppressWarnings("deprecation")
public class MTConnectedComponentsAlgo extends STConnectedComponentsAlgo {

    private long maxdDegreeINOUT=-1;
    private long maxDegreeID=-1;

    public MTConnectedComponentsAlgo(CCAlgorithmType type, boolean output){
        super(type, output);

    }


    @Override
    public void compute() {

        super.compute();

        if(myType==CCAlgorithmType.WEAK){
            MyBFS.createInstance().closeDownThreads();
        }
    }


    @Override
    protected void searchForWeakly(long n){

        MyBFS bfs = MyBFS.createInstance();
        Set<Long> reachableIDs = bfs.work(n, Direction.BOTH);

        for(Long l:reachableIDs){
            StartComparison.putIntoResultCounter(l, new AtomicInteger(componentID));
            allNodes.remove(l);
        }


    }

    /**
     *  Using the <b>Multistep Algorithm</b> described in the Paper  "BFS and Coloring-based Parallel Algorithms for
     *  Strongly Connected Components and Related Problems":
     *  <br><br>
     *  http://ieeexplore.ieee.org/xpl/articleDetails.jsp?arnumber=6877288
     */
    @Override
    protected void strongly(){
        // NOT YET IMPLEMENTED
        throw new NotImplementedException();
        // TODO: check/fix what happens if maxDegreeID is still on default value

        // PHASE 1
        /*
        // TODO: MyBFS should be used here
        Set<Long> D = BFS.go(maxDegreeID,Direction.OUTGOING);
        D.retainAll(BFS.go(maxDegreeID,Direction.INCOMING,D)); // D = S from Paper from here on

        for(Long l:D){
            StartComparison.putIntoResultCounter(l, new AtomicInteger(componentID));
            allNodes.remove(l);
        }
        componentID++;

        // PHASE 2

        // PHASE 3

        super.strongly(); // call seq. tarjan
        */
    }

    @Override
    protected void trimOrAddToAllNodes(Node n){
        if(n.getDegree()==0){
            // trivial CC
            StartComparison.putIntoResultCounter(n.getId(), new AtomicInteger(componentID));
            componentID++;

        } else{
            allNodes.add(n.getId());

            if(myType==CCAlgorithmType.STRONG){
                long degreeINOUT = n.getDegree(Direction.INCOMING)*n.getDegree(Direction.OUTGOING);
                if(degreeINOUT>maxdDegreeINOUT){
                    maxdDegreeINOUT =degreeINOUT;
                    maxDegreeID = n.getId();
                }
            }

        }
    }


}

