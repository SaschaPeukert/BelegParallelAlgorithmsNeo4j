package de.saschapeukert;

/**
 * Created by Sascha Peukert on 15.09.2015.
 */
public abstract class MyBaseRunnable implements Runnable {

    private Thread thisSingleton;

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
