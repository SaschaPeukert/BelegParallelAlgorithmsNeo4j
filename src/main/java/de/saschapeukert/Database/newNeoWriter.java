package de.saschapeukert.Database;

import de.saschapeukert.Algorithms.Abst.newMyBaseCallable;
import de.saschapeukert.newStartComparison;
import org.neo4j.kernel.api.DataWriteOperations;

/**
 * Created by Sascha Peukert on 05.09.2015.
 */
@SuppressWarnings("deprecation")
public class newNeoWriter extends newMyBaseCallable {
    // FIXME: HERE IS A PROBLEM!
    private final int propID;
    private final int startIndex; // inclusive
    private final int endIndex; // exclusive

    public newNeoWriter(int propID, int startIndex, int endIndex){
        super(false);
        this.propID = propID;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    @Override
    protected void compute() {
        //tx = db.openTransaction();
        DataWriteOperations ops = db.getDataWriteOperations();

        int count =0;
        for(int i = startIndex;i<endIndex;i++){
            if(count==100){
                db.closeTransactionWithSuccess(this.tx);
                this.tx = db.openTransaction();
                ops = db.getDataWriteOperations();
                count =0;
            }

            Long l = (Long) newStartComparison.getObjInResultCounterKeySet(i);
            db.createPropertyAtNode(l, newStartComparison.getResultCounterforId(l).longValue(), propID, ops);
            count++;
        }
        //db.closeTransactionWithSuccess(tx);
    }

    @Override
    public Object call() throws Exception {
        super.work();
        return null;
    }
}
