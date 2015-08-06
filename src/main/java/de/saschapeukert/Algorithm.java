package de.saschapeukert;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */
public abstract class Algorithm implements Runnable {

    public abstract void compute();

    @Override
    public void run() {
        compute();
    }

    public Thread getThread(){
        return new Thread(this);
    }
}
