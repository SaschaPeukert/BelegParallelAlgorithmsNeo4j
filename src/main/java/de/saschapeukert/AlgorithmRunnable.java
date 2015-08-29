package de.saschapeukert;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */
public abstract class AlgorithmRunnable implements Runnable {

    public Stopwatch timer;
    protected GraphDatabaseService graphDb;
    protected final int highestNodeId;
    public abstract void compute();

    @Override
    public void run() {
        compute();
    }

    /*
        This will also initialize the timer but NOT start it!
     */
    public AlgorithmRunnable(GraphDatabaseService gdb, int highestNodeId){
        this.timer = Stopwatch.createUnstarted();
        this.graphDb = gdb;
        this.highestNodeId = highestNodeId;

    }


    /**
     * This is just purely convinience
     */
    public Thread getNewThread(){
        return new Thread(this);
    }
}
