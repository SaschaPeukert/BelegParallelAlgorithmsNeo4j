package de.saschapeukert.Datastructures;

/**
 * Created by Sascha Peukert on 17.08.2015.
 */
public class TarjanNode {

    public int dfs;
    public int lowlink;
    public boolean onStack;

    public TarjanNode(){

        this.dfs = -1;
        this.lowlink = -1;
        this.onStack = false;
    }

}
