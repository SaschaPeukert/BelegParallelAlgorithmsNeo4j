package de.saschapeukert.Algorithms;

import com.google.common.base.Stopwatch;
import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */
public abstract class MyAlgorithmBaseRunnable extends MyBaseRunnable {

    public final Stopwatch timer;
    protected final GraphDatabaseService graphDb;
    protected final int highestNodeId;
    protected final boolean output;

    protected abstract void compute();

    @Override
    /**
     * It will automaticly open a TA
     */
    public void run() {
        tx = DBUtils.openTransaction(graphDb);
        compute();
        DBUtils.closeTransactionWithSuccess(tx);
    }


    /*
        This will also initialize the timer but NOT start it!
     */
    protected MyAlgorithmBaseRunnable(GraphDatabaseService gdb, int highestNodeId, boolean output){
        this.timer = Stopwatch.createUnstarted();
        this.graphDb = gdb;
        this.highestNodeId = highestNodeId;

        this.output = output;

    }

}
