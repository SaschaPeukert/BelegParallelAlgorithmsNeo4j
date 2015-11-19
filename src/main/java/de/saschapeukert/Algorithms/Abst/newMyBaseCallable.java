package de.saschapeukert.Algorithms.Abst;

import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.Transaction;

import java.util.concurrent.Callable;

/**
 * Created by Sascha Peukert on 15.09.2015.
 */
public abstract class newMyBaseCallable implements Callable {

    protected Transaction tx;
    protected DBUtils db;
    protected Boolean output;

    protected abstract void compute();

    public newMyBaseCallable(boolean output){
        this.db = DBUtils.getInstance("","");
        this.output = output;
    }

    protected void work(){
        this.tx = db.openTransaction();
        compute();
        db.closeTransactionWithSuccess(this.tx);
    }

}
