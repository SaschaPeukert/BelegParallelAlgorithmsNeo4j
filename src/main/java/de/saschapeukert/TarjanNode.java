package de.saschapeukert;

import org.neo4j.graphdb.Node;

/**
 * Created by Sascha Peukert on 17.08.2015.
 */
public class TarjanNode {

    public Node node;
    public int dfs;
    public int lowlink;
    public boolean onStack;

    public TarjanNode(Node n){
        this.node = n;
        this.dfs = -1;
        this.lowlink = -1;
        this.onStack = false;
    }

}
