package de.saschapeukert;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sascha Peukert on 01.12.2015.
 */
public class Utils {
    public static boolean waitForExecutorToFinishAll(ExecutorService executor){
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
