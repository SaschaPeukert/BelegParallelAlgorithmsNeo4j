package de.saschapeukert.Algorithms.Abst;

import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.Transaction;

import java.util.concurrent.Callable;

/**
 * Created by Sascha Peukert on 19.11.2015.
 */
public abstract class MyBaseCallable implements Callable {

    protected Transaction tx;
    protected DBUtils db;
    protected Boolean output;

    protected abstract void compute();

    public MyBaseCallable(boolean output){
        this.db = DBUtils.getInstance("","");
        this.output = output;
    }

    protected void work(){
        this.tx = db.openTransaction();
        compute();
        db.closeTransactionWithSuccess(this.tx);
    }

}
