package de.saschapeukert.Algorithms.Impl.ConnectedComponents.Search;

import com.carrotsearch.hppc.cursors.LongCursor;
import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * This class represents a DFS to be used in Algorithms
 * <br>
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

        Iterator<LongCursor> it =db.getConnectedNodeIDs(ops, n, direction).iterator();
        while(it.hasNext()){
            Long l = it.next().value;
            doDFS(l,direction);
        }
    }
}