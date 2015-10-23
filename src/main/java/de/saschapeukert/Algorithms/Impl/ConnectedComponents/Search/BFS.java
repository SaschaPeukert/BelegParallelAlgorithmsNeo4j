package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by Sascha Peukert on 04.10.2015.
 */
public class BFS {

    private static DBUtils db;

    public static Set<Long> go(long nodeID, Direction direction){

        db = DBUtils.getInstance("","");
        ReadOperations ops =db.getReadOperations();

        Set<Long> visitedIDs = new HashSet<>();
        List<Long> frontierList= new LinkedList<>();

        visitedIDs.clear();

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

    // TODO: Implement this in myBFS too

    /**
     * Only does BFS on the nodes specified in nodeIDSet
     * @param nodeID
     * @param direction
     * @param nodeIDSet
     * @return
     */
    public static Set<Long> go(long nodeID, Direction direction, Set<Long> nodeIDSet){

        db = DBUtils.getInstance("","");
        ReadOperations ops =db.getReadOperations();

        Set<Long> visitedIDs = new HashSet<>();
        List<Long> frontierList= new LinkedList<>();

        visitedIDs.clear();

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
