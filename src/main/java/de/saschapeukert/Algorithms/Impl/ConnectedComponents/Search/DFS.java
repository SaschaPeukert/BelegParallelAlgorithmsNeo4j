package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sascha Peukert on 11.10.2015.
 */
public class DFS {

    private static Set<Long> visitedIDs = new HashSet<>();
    private static DBUtils db;
    private static ReadOperations ops;
    public static Set<Long> go(Long n, Direction direction) {

        db = DBUtils.getInstance("","");
        visitedIDs = new HashSet<>();
        ops =db.getReadOperations();

        doDFS(n, direction);
        return visitedIDs;
    }

    private static void doDFS(Long n, Direction direction) {
        if (visitedIDs.contains(n)) {
            return;
        }
        visitedIDs.add(n);

        for (Long l : db.getConnectedNodeIDs(ops, n, direction)) {
            doDFS(l,direction);
        }
    }
}