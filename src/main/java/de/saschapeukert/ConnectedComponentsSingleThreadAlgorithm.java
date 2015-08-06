package de.saschapeukert;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */
public class ConnectedComponentsSingleThreadAlgorithm extends AlgorithmRunnable {


    private Map<Integer,List<Node>> components;

    public ConnectedComponentsSingleThreadAlgorithm(GraphDatabaseService gdb){
        super(gdb);
    }


    @Override
    public void compute() {

        timer.start();

        // TODO: implement BFS or DFS

        timer.stop();

    }

    public String printResults(){

        StringBuilder returnString = new StringBuilder();
        returnString.append("Component count: " + components.keySet().size() + "\n");
        returnString.append("- - - - -\n");
        for(int i:components.keySet()){
            boolean first = true;
            returnString.append("Component " + i + ": ");
            for(Node n:components.get(i)){
                if(!first){
                    returnString.append(", ");
                }
                returnString.append(n.getId());
            }
            returnString.append("\n");
        }

        returnString.append("- - - - -\n");
        returnString.append("Done in: " + timer.elapsed(TimeUnit.MICROSECONDS)+ "\u00B5s");

        return returnString.toString();
    }

}
