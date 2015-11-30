package de.saschapeukert.Algorithms.Abst;

import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;

/**
 * Created by Sascha Peukert on 19.11.2015.
 */
public abstract class myAlgorithmBaseCallable extends MyBaseCallable {

    public final Stopwatch timer;
    protected boolean output;
    protected final TimeUnit timeUnit;

    /*
        This will also initialize the timer but NOT start it!
     */
    protected myAlgorithmBaseCallable(TimeUnit timeUnit){
        this.timer = Stopwatch.createUnstarted();
        this.timeUnit = timeUnit;
    }

    @Override
    /**
     *  returns the elapsed time of the timer
     */
    public Object call() throws Exception {
        work();
        return timer.elapsed(timeUnit);
    }
}
