package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Algorithms.MyAlgorithmBaseRunnable;
import de.saschapeukert.Database.DBUtils;
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
    public Set<Long> ignoreIDs;
    private ReadOperations ops;
    private int posInList;

    public volatile boolean isAlive = true;

    public BFSLevelRunnable(int pos,Direction direction, GraphDatabaseService gdb, boolean output){
        super(gdb,output);
        this.direction = direction;
        posInList = pos;  // MAY NOT BE 0 !
    }

    @Override
    protected void compute() {
        ops = DBUtils.getReadOperations();
        while(true){
            synchronized (BFS.frontierList){
                try {
                    BFS.frontierList.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                if(!isAlive) return;

                int i=1; //counter
                boolean nullFound=false;

                while(!nullFound){

                    try{
                        parentID = BFS.frontierList.get(i*posInList);
                        BFS.MapOfQueues.put(i*posInList,expandNode(parentID));

                    } catch (IndexOutOfBoundsException e){
                        nullFound=true;
                    }

                    i++;
                }

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

}


