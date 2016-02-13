package de.saschapeukert.Algorithms.Abst;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongLookupContainer;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

/**
 * This class is the basis of all Callables that need to perform work in parallel on distinct parts of ans array.
 * Concrete classes that extend this need to implement work()! All operations done in work() will automatically be done
 * within a transaction.
 * <br>
 * Created by Sascha Peukert on 19.11.2015.
 */
public abstract class WorkerCallableTemplate extends MyBaseCallable {

    protected long[] refArray;
    protected int startPos; //incl.
    protected int endPos; // not incl.

    protected LongArrayList returnList; // Set vorher
    private ReadOperations ops;

    protected WorkerCallableTemplate(int startPos, int endPos, long[] array) {

        this.refArray = array;
        this.startPos = startPos;
        this.endPos = endPos;
        returnList = new LongArrayList(10000);
    }

    public Object call() throws Exception {
        tx = db.openTransaction();
        ops = db.getReadOperations(); // needs to be in a TA
        work();
        db.closeTransactionWithSuccess(tx);
        return returnList;
    }

    protected LongHashSet expandNode(Long id, LongLookupContainer c, boolean ignoreIfCollectionsContainsItem, Direction dir){
        LongHashSet resultSet = expandNode(id,dir);
                //new HashSet<>(db.getConnectedNodeIDs(ops, id, dir));
        if(ignoreIfCollectionsContainsItem){
            // nicht aufnehmen
            resultSet.removeAll(c);
        } else{
            // nur aufnehmen, wenn drin
            resultSet.retainAll(c);
        }
        return resultSet;
    }

    protected LongArrayList expandNodeAsList(Long id, LongLookupContainer c, boolean ignoreIfCollectionsContainsItem, Direction dir){
        LongArrayList resultSet = expandNodeAsList(id,dir);
        //new HashSet<>(db.getConnectedNodeIDs(ops, id, dir));
        if(ignoreIfCollectionsContainsItem){
            // nicht aufnehmen
            resultSet.removeAll(c);
        } else{
            // nur aufnehmen, wenn drin
            resultSet.retainAll(c);
        }
        return resultSet;
    }

    protected LongHashSet expandNode(Long id, Direction dir){
        return new LongHashSet(db.getConnectedNodeIDs(ops, id, dir));
    }

    protected LongArrayList expandNodeAsList(Long id, Direction dir){
        return new LongArrayList(db.getConnectedNodeIDsAsList(ops, id, dir));
    }

}
