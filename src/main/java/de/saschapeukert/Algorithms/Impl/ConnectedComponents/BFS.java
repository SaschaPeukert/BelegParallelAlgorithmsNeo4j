package de.saschapeukert.Algorithms.Impl.ConnectedComponents;

import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.Direction;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Sascha Peukert on 04.10.2015.
 */
public class BFS {

    private Queue<Long> queue;

    public BFS(){
        queue = new ConcurrentLinkedQueue<Long>();

    }

    /**
     * Does the BFS
     * @param nodeID The NodeID where the search will start
     * @param direction The direction the edges will be traveled
     * @return a Set of all reachable NodeIDs
     */
    public Set<Long> go(long nodeID, Direction direction){
        Set<Long> visitedIDs = new HashSet<>();

        queue.add(nodeID);
        //printNode(this.rootNode);
        visitedIDs.add(nodeID);

        while(!queue.isEmpty())
        {
            Long n=queue.remove();

            for(Long child: DBUtils.getConnectedNodeIDs(ConnectedComponentsSingleThreadAlgorithm.ops, nodeID, direction)){
                visitedIDs.add(child);
                queue.add(child);
            }

        }

        return visitedIDs;
    }
}
