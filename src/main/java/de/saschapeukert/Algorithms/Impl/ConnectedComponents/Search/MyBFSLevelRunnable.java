package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import de.saschapeukert.Algorithms.Abst.WorkerRunnable;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;

import java.util.Queue;

/**
 * Created by Sascha Peukert on 04.10.2015.
 */
public class MyBFSLevelRunnable extends WorkerRunnable {


    public Direction direction;
    private final int posInList;
    private int i; //counter

    public MyBFSLevelRunnable(int pos, boolean output){
        super(output);

        posInList = pos;
        i=0;
    }

    @Override
    protected boolean operation(){

        try {
            int key = ((i * StartComparison.NUMBER_OF_THREADS) + posInList);
            parentID = MyBFS.frontierList.get(key);
            Queue<Long> q = expandNode(parentID,MyBFS.visitedIDs,true,direction);

            MyBFS.MapOfQueues.put(key, q);
            MyBFS.visitedIDs.addAll(q);

            //System.out.println("Done " + parentID + " (T" + posInList + ")");
            i++;

            } catch (IndexOutOfBoundsException e) {
                return false;
            } catch (Exception e){
                e.printStackTrace();
            }
        return true;
    }

    @Override
    protected void cleanUpOperation(){
        MyBFS.setCheckThreadList(posInList);
    }


}


