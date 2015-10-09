package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Algorithms.MyAlgorithmBaseRunnable;
import de.saschapeukert.Database.DBUtils;
import de.saschapeukert.StartComparison;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.ReadOperations;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * Created by Sascha Peukert on 04.10.2015.
 */
public class BFSLevelRunnable extends MyAlgorithmBaseRunnable {

    public long parentID;
    private Direction direction;
    public volatile Set<Long> ignoreIDs;
    private ReadOperations ops;
    private int posInList;

    public volatile boolean isAlive = true;
    private volatile boolean isWaiting = true;

    public BFSLevelRunnable(int pos,Direction direction, GraphDatabaseService gdb, boolean output){
        super(gdb,output);
        this.direction = direction;
        posInList = pos;  // MAY NOT BE 0 !
    }

    @Override
    protected void compute() {
        ops = DBUtils.getReadOperations();
        System.out.println("Thread " + posInList + " alive");
        synchronized (BFS.frontierList) {
            while (isAlive) {
                try {
                    System.out.println("Thread " + posInList + " waiting");

                    setWaiting(true);
                    BFS.frontierList.wait();

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                setWaiting(false);
                System.out.println("Thread " + posInList + " active");
                if (!isAlive) {
                    System.out.println("Thread " + posInList + " killed");
                    return;
                }

                if (posInList == 1) {
                    System.out.print("");
                }

                int i = 0; //counter
                boolean nullFound = false;

                while (!nullFound) {

                    try {

                        parentID = BFS.frontierList.get(i * StartComparison.NUMBER_OF_THREADS + (posInList-1));
                        BFS.MapOfQueues.put(i * StartComparison.NUMBER_OF_THREADS + (posInList-1), expandNode(parentID));

                    } catch (IndexOutOfBoundsException e) {
                        nullFound = true;
                    }

                    i++;
                }
                System.out.println("Thread " + posInList + " done");

            }

        }
     }

    private Queue<Long> expandNode(Long id){
        Queue<Long> resultQueue = new LinkedList<Long>();
        for(Long child: DBUtils.getConnectedNodeIDs(ops, parentID, direction)){
            if(ignoreIDs.contains(child)) continue;

            resultQueue.add(child);
        }
        return resultQueue;
    }

    public synchronized boolean isWaiting(){
        return isWaiting;
    }

    private synchronized void setWaiting(boolean b){
         isWaiting =b;
    }
}


