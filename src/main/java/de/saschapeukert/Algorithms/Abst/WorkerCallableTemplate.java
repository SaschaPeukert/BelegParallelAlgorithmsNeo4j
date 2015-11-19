package de.saschapeukert.Algorithms.Abst;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sascha Peukert on 23.10.2015.
 */
public abstract class WorkerCallableTemplate extends newMyBaseCallable {
    protected WorkerCallableTemplate( boolean output) {
        super(output);
    }

    // Children must overwrite compute()

    protected long parentID;
    protected Set<Long> returnSet;
    private ReadOperations ops;

    public Object call() throws Exception {
        ops = db.getReadOperations();
        work();
        return returnSet;
    }

    protected Set<Long> expandNode(Long id, Collection c, boolean contains, Direction dir){
        Set<Long> resultQueue = new HashSet<>(db.getConnectedNodeIDs(ops, id, dir));

        if(contains){
                // nicht aufnehmen
            resultQueue.removeAll(c);
        } else{
                // nur aufnehmen, wenn drin
            resultQueue.retainAll(c);
        }
        return resultQueue;
    }

}
