package de.saschapeukert.algorithms.impl.connected.components.search;

import com.carrotsearch.hppc.LongCollection;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import de.saschapeukert.database.DBUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.*;

/**
 * This class represents a BFS to be used in algorithms
 * <br>
 * Created by Sascha Peukert on 04.10.2015.
 */
public class BFS {

    private static DBUtils db;

    public static LongHashSet go(long nodeID, Direction direction, DBUtils db){

        ReadOperations ops =db.getReadOperations();

        LongHashSet visitedIDs = new LongHashSet();
        List<Long> frontierList= new LinkedList<>();

        frontierList.add(nodeID);
        visitedIDs.add(nodeID);

        while(!frontierList.isEmpty())
        {
            Long n = frontierList.remove(0);

            Iterator<LongCursor> it = db.getConnectedNodeIDs(ops, n, direction).iterator();
            while(it.hasNext()){
                Long child = it.next().value;
                if(visitedIDs.contains(child)){
                    continue;
                }
                visitedIDs.add(child);
                frontierList.add(child);
            }
        }
        return visitedIDs;
    }

    /**
     * Only does BFS on the nodes specified in nodeIDSet
     * @param nodeID
     * @param direction
     * @param nodeIDSet
     * @return
     */
    public static LongHashSet go(long nodeID, Direction direction, LongCollection nodeIDSet, DBUtils db){

        ReadOperations ops =db.getReadOperations();

        LongHashSet visitedIDs = new LongHashSet();
        List<Long> frontierList= new LinkedList<>();

        frontierList.add(nodeID);
        visitedIDs.add(nodeID);

        while(!frontierList.isEmpty())
        {
            Long n = frontierList.remove(0);
            Iterator<LongCursor> it = db.getConnectedNodeIDs(ops, n, direction).iterator();
            while(it.hasNext()){
                Long child = it.next().value;
                if(visitedIDs.contains(child)){
                    continue;
                }
                if(!nodeIDSet.contains(child)){
                    continue;
                }
                visitedIDs.add(child);
                frontierList.add(child);
            }
        }
        return visitedIDs;
    }
}
