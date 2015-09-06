package de.saschapeukert;

import com.google.common.base.Stopwatch;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */
public abstract class AlgorithmRunnable implements Runnable {

    public Stopwatch timer;
    protected GraphDatabaseService graphDb;
    protected final int highestNodeId;
    protected final int propID;
    protected final String propName;

    protected final Map<Long,Integer> result;

    public abstract void compute();

    @Override
    public void run() {
        compute();
    }

    /*
        This will also initialize the timer but NOT start it!
     */
    public AlgorithmRunnable(GraphDatabaseService gdb, int highestNodeId, int propID, String propName){
        this.timer = Stopwatch.createUnstarted();
        this.graphDb = gdb;
        this.highestNodeId = highestNodeId;
        this.propID = propID;
        this.propName = propName;
        this.result = new LinkedHashMap<>(highestNodeId);
    }


    /**
     * This is just purely convinience
     */
    public Thread getNewThread(){
        return new Thread(this);
    }
}
