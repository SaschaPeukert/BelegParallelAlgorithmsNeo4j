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

    public MyBaseCallable(){
        this.db = DBUtils.getInstance("","");
    }

    public abstract void work();
}
