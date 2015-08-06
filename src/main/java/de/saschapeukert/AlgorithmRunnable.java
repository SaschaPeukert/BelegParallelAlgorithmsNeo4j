package de.saschapeukert;

import com.google.common.base.Stopwatch;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */
public abstract class AlgorithmRunnable implements Runnable {

    public Stopwatch timer;

    public abstract void compute();

    @Override
    public void run() {
        compute();
    }


    /*
    This is just purely convinience
     */
    public Thread getNewThread(){
        return new Thread(this);
    }
}
