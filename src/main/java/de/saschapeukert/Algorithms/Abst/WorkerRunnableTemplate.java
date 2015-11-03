package de.saschapeukert.Algorithms.Abst;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.ReadOperations;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by Sascha Peukert on 23.10.2015.
 */
public abstract class WorkerRunnableTemplate extends MyAlgorithmBaseRunnable {
    protected WorkerRunnableTemplate(boolean output) {
        super(output);
    }

    protected volatile long parentID;
    private ReadOperations ops;

    public final AtomicBoolean isAlive = new AtomicBoolean(true);
    public final AtomicBoolean isIdle = new AtomicBoolean(true);

    @Override
    protected void compute() {
        ops = db.getReadOperations();
        //System.out.println("Thread " + posInList + " alive");
        while (isAlive.get()) {

            //System.out.println("Thread " + posInList + " waiting");
            while (isIdle.get()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            //System.out.println("Thread " + posInList + " active");
            if (!isAlive.get()) {
                //System.out.println("Thread " + posInList + " killed");
                return;
            }
            //noinspection StatementWithEmptyBody,StatementWithEmptyBody
            while (operation()) {
                //work
            }
            cleanUpOperation();
            //System.out.println("Thread " + posInList + " done");
        }
    }

    protected Queue<Long> expandNode(Long id, Collection c, boolean contains, Direction dir){
        Queue<Long> resultQueue = new LinkedList<>();

        for(Long child: db.getConnectedNodeIDs(ops, id, dir)){
            if(contains){
                if(c.contains(child)) continue;
            } else{
                if(!c.contains(child)) continue;
            }
            resultQueue.add(child);
        }
        return resultQueue;
    }

    protected void cleanUpOperation(){
        isIdle.set(true); // now it waits after one operation and can be used again
    }
    protected abstract boolean operation();

    @Override
    protected void initialize(){}
}
