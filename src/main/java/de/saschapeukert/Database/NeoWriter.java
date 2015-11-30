package de.saschapeukert.Database;

import de.saschapeukert.newStartComparison;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.DataWriteOperations;

/**
 * Created by Sascha Peukert on 05.09.2015.
 */
@SuppressWarnings("deprecation")
public class NeoWriter implements Runnable {
    // FIXME:Problem solved?
    private final int propID;
    private final int startIndex; // inclusive
    private final int endIndex; // exclusive
    private final DBUtils db;

    public NeoWriter(int propID, int startIndex, int endIndex, DBUtils db){
        this.propID = propID;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.db = db;
    }

    @Override
    public void run() {
        Transaction tx = db.openTransaction();
        DataWriteOperations ops = db.getDataWriteOperations();

        for(int i = startIndex;i<endIndex;i++){
            Long l = (Long) newStartComparison.getObjInResultCounterKeySet(i);
            db.createPropertyAtNode(l, newStartComparison.getResultCounterforId(l).longValue(), propID, ops);
        }
        db.closeTransactionWithSuccess(tx);
    }
}
