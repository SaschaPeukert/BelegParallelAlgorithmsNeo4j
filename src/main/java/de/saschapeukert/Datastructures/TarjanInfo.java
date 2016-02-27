package de.saschapeukert.datastructures;

/**
 * This class is just a helper for all the attributes need to perform tarjans algorithm (SCC).
 * <br>
 * Created by Sascha Peukert on 17.08.2015.
 */
public class TarjanInfo {

    public int dfs;
    public int lowlink;
    public boolean onStack;

    public TarjanInfo(){
        this.dfs = -1;
        this.lowlink = -1;
        this.onStack = false;
    }

}
