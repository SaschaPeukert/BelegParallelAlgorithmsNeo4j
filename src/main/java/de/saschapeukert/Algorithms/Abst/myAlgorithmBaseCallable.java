package de.saschapeukert.Algorithms.Abst;

import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;

/**
 * This class is the basis of all used Algorithms.
 * Additionaly it provides a stopwatch to measure runtimes.
 * <br>
 * Concrete classes that extend this need to implement work()!
 * <br>
 * Created by Sascha Peukert on 19.11.2015.
 */
public abstract class MyAlgorithmBaseCallable extends MyBaseCallable {

    public final Stopwatch timer;
    protected final TimeUnit timeUnit;

    /*
        This will also initialize the timer but NOT start it!
     */
    protected MyAlgorithmBaseCallable(TimeUnit timeUnit){
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
