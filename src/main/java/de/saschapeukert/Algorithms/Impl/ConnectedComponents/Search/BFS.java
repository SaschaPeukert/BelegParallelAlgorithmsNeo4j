package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.*;

/**
 * This class represents a BFS to be used in Algorithms
 * <br>
 * Created by Sascha Peukert on 04.10.2015.
 */
public class BFS {

    private static DBUtils db;

    public static Set<Long> go(long nodeID, Direction direction){

        db = DBUtils.getInstance("","");
        ReadOperations ops =db.getReadOperations();

        Set<Long> visitedIDs = new HashSet<>();
        List<Long> frontierList= new LinkedList<>();

        frontierList.add(nodeID);
        visitedIDs.add(nodeID);

        while(!frontierList.isEmpty())
        {
            Long n = frontierList.remove(0);

            for(Long child: db.getConnectedNodeIDs(ops, n, direction)){
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
    public static Set<Long> go(long nodeID, Direction direction, Collection<Long> nodeIDSet){

        db = DBUtils.getInstance("","");
        ReadOperations ops =db.getReadOperations();

        Set<Long> visitedIDs = new HashSet<>();
        List<Long> frontierList= new LinkedList<>();

        frontierList.add(nodeID);
        visitedIDs.add(nodeID);

        while(!frontierList.isEmpty())
        {
            Long n = frontierList.remove(0);
            for(Long child: db.getConnectedNodeIDs(ops, n, direction)){
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
