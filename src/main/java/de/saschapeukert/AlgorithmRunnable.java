package de.saschapeukert;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */
public abstract class AlgorithmRunnable implements Runnable {

    public Stopwatch timer;
    protected GraphDatabaseService graphDb;

    public abstract void compute();

    @Override
    public void run() {
        compute();
    }

    /*
        This will also initialize the timer but NOT start it!
     */
    public AlgorithmRunnable(GraphDatabaseService gdb){
        this.timer = Stopwatch.createUnstarted();
        this.graphDb = gdb;
    }


    /*
    This is just purely convinience
     */
    public Thread getNewThread(){
        return new Thread(this);
    }
}
