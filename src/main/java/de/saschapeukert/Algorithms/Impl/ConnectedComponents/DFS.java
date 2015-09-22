package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Sascha Peukert on 17.09.2015.
 */
class DFS {

    private Long currentNodeID;
    private int id;
    //private List<Long> visited;

    public DFS(int max){
        //this.visited = new ArrayList<Long>(max/2);
    }

    public void setCurrentNodeID(Long newId){
        this.currentNodeID = newId;
    }

    public void setId(int id){
        this.id = id;
    }
    /*public void resetList(){
        visited.clear();
    }*/

    private void go(int barrier){

        if(barrier<=0)
            return;

        //Stopwatch timer = Stopwatch.createStarted();
        AtomicInteger aIntID = StartComparison.resultCounter.get(currentNodeID);
        int oldID = aIntID.intValue();
        //timer.stop();
        //System.out.println(timer.elapsed(TimeUnit.NANOSECONDS));
        if(oldID!=0){
            // Already visted

            if(oldID!=id){
                // not by myself! Rewrite!
                id = oldID;
                //for(Long k:visited){
                //    StartComparison.resultCounter.put(k,aIntID);
                //}

            } // else: by myself

            return;
        }// else: not yet visited. Lets do that now:

        //visited.add(currentNodeID);
        StartComparison.resultCounter.put(currentNodeID, new AtomicInteger(id));
        ConnectedComponentsSingleThreadAlgorithm.allNodes.remove(currentNodeID); //  notwendig?!

        int barrier_new = barrier-1;
        for(Long l: DBUtils.getConnectedNodeIDs(ConnectedComponentsSingleThreadAlgorithm.ops, currentNodeID, Direction.BOTH)){
            currentNodeID = l;
            go(barrier_new);
        }
    }
}
