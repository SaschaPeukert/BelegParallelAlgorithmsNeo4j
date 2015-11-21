package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import de.saschapeukert.Algorithms.Abst.WorkerRunnableTemplate;
import org.neo4j.graphdb.Direction;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sascha Peukert on 04.10.2015.
 */
public class MyBFSLevelRunnable extends WorkerRunnableTemplate {

    public Direction direction;
    private final int posInList;
    //private int i; //counter
    public final Set<Long> resultQueue;

    public MyBFSLevelRunnable(int pos, boolean output){
        super(output);

        resultQueue = new HashSet<>(100000);
        posInList = pos;

    }

    @Override
    protected boolean operation(){

        try {
            //int key = ((i * StartComparison.NUMBER_OF_THREADS) + posInList);
            parentID = MyBFS.getKey();

            if(parentID==-1){
                return false;
            }


            Set<Long> q = expandNode(parentID,MyBFS.visitedIDs,true,direction);
            MyBFS.visitedIDs.add(parentID);

            if(MyBFS.nodeIDSet!=null){
               // q.add(parentID); // quick fix // TODO TEST THIS
                q.retainAll(MyBFS.nodeIDSet);
            }

            q.remove(parentID);

            resultQueue.addAll(q);

            //System.out.println("Done " + parentID + " (T" + posInList + ")");
            //i++;

            } catch (Exception e){
                e.printStackTrace();
            }
        return true;
    }

    @Override
    protected void cleanUpOperation(){
        isIdle.set(true);
    }
}


