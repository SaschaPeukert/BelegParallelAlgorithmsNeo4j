package de.saschapeukert.Algorithms;

import org.neo4j.graphdb.Transaction;

/**
 * Created by Sascha Peukert on 15.09.2015.
 */
public abstract class MyBaseRunnable implements Runnable {

    private Thread thisSingleton;

    protected Transaction tx;

    /**
     * This is just purely convinience
     */
    public Thread getThread(){
        if(thisSingleton==null) {
            thisSingleton = new Thread(this);
        }

        return thisSingleton;
    }
}
