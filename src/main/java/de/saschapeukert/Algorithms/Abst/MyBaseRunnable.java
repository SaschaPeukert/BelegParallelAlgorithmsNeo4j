package de.saschapeukert.Algorithms.Abst;

import org.neo4j.graphdb.Transaction;

/**
 * Created by Sascha Peukert on 15.09.2015.
 */
public abstract class MyBaseRunnable implements Runnable {

    protected Transaction tx;

}
