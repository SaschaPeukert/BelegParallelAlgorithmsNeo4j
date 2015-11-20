package de.saschapeukert.Algorithms.Abst;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sascha Peukert on 19.11.2015.
 */
public abstract class WorkerCallableTemplate extends newMyBaseCallable {

    protected Long[] refArray;
    protected int startPos; //incl.
    protected int endPos; // not incl.

    protected Set<Long> returnSet;
    private ReadOperations ops;

    protected WorkerCallableTemplate(int startPos, int endPos, Long[] array, boolean output) {
        super(output);
        this.refArray = array;
        this.startPos = startPos;
        this.endPos = endPos;
        returnSet = new HashSet<>(10000);
    }

    // Children must overwrite compute()



    public Object call() throws Exception {
        ops = db.getReadOperations();
        work();
        return returnSet;
    }

    protected Set<Long> expandNode(Long id, Collection c, boolean ignoreIfCollectionsContainsItem, Direction dir){
        Set<Long> resultSet = new HashSet<>(db.getConnectedNodeIDs(ops, id, dir));

        if(ignoreIfCollectionsContainsItem){
            // nicht aufnehmen
            resultSet.removeAll(c);
        } else{
            // nur aufnehmen, wenn drin
            resultSet.retainAll(c);
        }
        return resultSet;
    }

}
