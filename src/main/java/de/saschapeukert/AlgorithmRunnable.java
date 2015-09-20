package de.saschapeukert;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */
public abstract class AlgorithmRunnable extends MyBaseRunnable {

    public Stopwatch timer;
    protected GraphDatabaseService graphDb;
    protected final int highestNodeId;
    protected final boolean output;

    public abstract void compute();

    @Override
    /**
     * It will automaticly open a TA
     */
    public void run() {
        DBUtils.openTransaction(graphDb);
        compute();
        DBUtils.closeTransactionSuccess(tx);
    }


    /*
        This will also initialize the timer but NOT start it!
     */
    public AlgorithmRunnable(GraphDatabaseService gdb, int highestNodeId, boolean output){
        this.timer = Stopwatch.createUnstarted();
        this.graphDb = gdb;
        this.highestNodeId = highestNodeId;

        this.output = output;

    }

}
