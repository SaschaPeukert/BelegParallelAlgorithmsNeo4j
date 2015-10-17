package de.saschapeukert.Algorithms.Abst;

import com.google.common.base.Stopwatch;
import de.saschapeukert.Database.DBUtils;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */
public abstract class MyAlgorithmBaseRunnable extends MyBaseRunnable {

    public final Stopwatch timer;
    protected final DBUtils db;
    protected final boolean output;

    protected abstract void compute();


    @Override
    /**
     * It will automaticly open a TA
     */
    public void run() {
        this.tx = db.openTransaction();
        compute();
        db.closeTransactionWithSuccess(this.tx);
    }


    /*
        This will also initialize the timer but NOT start it!
     */
    protected MyAlgorithmBaseRunnable(boolean output){
        this.timer = Stopwatch.createUnstarted();
        this.db = DBUtils.getInstance("","");
        this.output = output;

    }

}
