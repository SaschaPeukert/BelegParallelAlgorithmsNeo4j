package de.saschapeukert.Datastructures;

/**
 * Created by Sascha Peukert on 17.08.2015.
 */
public class TarjanNode {

    // --Commented out by Inspection (21.09.2015 15:07):private final Node node;
    public int dfs;
    public int lowlink;
    public boolean onStack;

    public TarjanNode(){//Node n){
        //this.node = n;
        this.dfs = -1;
        this.lowlink = -1;
        this.onStack = false;
    }

}
