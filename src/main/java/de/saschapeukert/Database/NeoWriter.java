package de.saschapeukert.Database;

import de.saschapeukert.Algorithms.MyBaseRunnable;
import de.saschapeukert.StartComparison;
import org.neo4j.kernel.api.DataWriteOperations;

/**
 * Created by Sascha Peukert on 05.09.2015.
 */
@SuppressWarnings("deprecation")
public class NeoWriter extends MyBaseRunnable {


    private final DBUtils db;
    private final int propID;
    private final int startIndex; // inclusive
    private final int endIndex; // exclusive

    public NeoWriter( int propID, int startIndex, int endIndex){

        this.db = DBUtils.getInstance("","");
        this.propID = propID;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    @Override
    public void run() {

            tx = db.openTransaction();

            DataWriteOperations ops = db.getDataWriteOperations();

            int count =0;
            for(int i = startIndex;i<endIndex;i++){

                if(count==100){

                    db.closeTransactionWithSuccess(tx);

                    tx = db.openTransaction();

                    ops = db.getDataWriteOperations();

                    count =0;
                }

                Long l = (Long) StartComparison.getObjInResultCounterKeySet(i);
                db.createIntPropertyAtNode(l, StartComparison.getResultCounterforId(l).intValue(), propID, ops);
                count++;

            }

            db.closeTransactionWithSuccess(tx);


    }

}
