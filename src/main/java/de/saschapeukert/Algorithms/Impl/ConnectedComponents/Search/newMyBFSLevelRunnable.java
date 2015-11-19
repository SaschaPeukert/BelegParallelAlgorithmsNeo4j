package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import de.saschapeukert.Algorithms.Abst.WorkerRunnableTemplate;
import org.neo4j.graphdb.Direction;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sascha Peukert on 04.10.2015.
 */
public class newMyBFSLevelRunnable extends WorkerRunnableTemplate {

    public Direction direction;
    private final int startPos;
    private final int endPos;
    //private int i; //counter
    public final Set<Long> resultQueue;
    private final Set<Long> privateQueue;
    private final Long[] array;

    public newMyBFSLevelRunnable(int startPos, int endPos, Long[] array, Direction direction, boolean output){
        super(output);

        this.array = array;
        this.direction = direction;
        resultQueue = new HashSet<>(100000);
        privateQueue = new HashSet<>(100000);
        this.startPos = startPos;
        this.endPos = endPos;

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
                q.add(parentID); // quick fix
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


