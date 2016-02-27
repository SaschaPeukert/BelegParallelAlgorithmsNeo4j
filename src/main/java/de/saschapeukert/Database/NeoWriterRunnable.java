package de.saschapeukert.database;

import de.saschapeukert.Starter;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.DataWriteOperations;

/**
 * This class represents a NeoWriter.
 * It's function is described in Section 4.1 of my study project document.
 * <br>
 * Created by Sascha Peukert on 05.09.2015.
 */
@SuppressWarnings("deprecation")
public class NeoWriterRunnable implements Runnable {
    private final int propID;
    private final int startIndex; // inclusive
    private final int endIndex; // exclusive
    private final DBUtils db;

    public NeoWriterRunnable(int propID, int startIndex, int endIndex, DBUtils db){
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
            Long l = (Long) Starter.getObjInResultCounterKeySet(i);
            db.createPropertyAtNode(l, Starter.getResultCounterforId(l).longValue(), propID, ops);
        }
        db.closeTransactionWithSuccess(tx);
    }
}
