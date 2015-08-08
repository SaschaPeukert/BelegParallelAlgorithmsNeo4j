package de.saschapeukert;

import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */
public class ConnectedComponentsSingleThreadAlgorithm extends AlgorithmRunnable {


    private Map<Node,String> components;


    public ConnectedComponentsSingleThreadAlgorithm(GraphDatabaseService gdb){
        super(gdb);
        components = new HashMap<Node, String>();

    }


    @Override
    public void compute() {

        timer.start();

        int componentID = 0;


        try (Transaction tx = graphDb.beginTx()) {
            GlobalGraphOperations operations = GlobalGraphOperations.at(graphDb);
            ResourceIterable<Node> it = operations.getAllNodes();

            for (Node n : it) {

                // Every node has to be marked as (part of) a component
                if(components.get(n) == null){
                    DFS(n,"C"+componentID);
                    componentID++;
                }

            }

            tx.success();
        }

        timer.stop();


    }


    private void DFS(Node n, String compName){
        //String rString = "";

        if(components.get(n)==compName){
            return;// rString; // Already visited
        }
        /*
        if(components.get(n)!=null){
            return;// components.get(n);  // THIS WILL NEVER HAPPEN
        }
        */
        // NOW IT HAS TO BE NULL
        components.put(n, compName);

        for(Relationship r :n.getRelationships()){
            DFS(r.getOtherNode(n),compName);

        }
        /*
        // USELESS EXPERIMENTAL STUFF
        String s = "Lets go";
        while(s!=""){
            s="";
            for(Relationship r :n.getRelationships()){
                s = DFS(r.getOtherNode(n),compName);

                if(s!=""){
                    components.put(n,s);
                    break;
                }
            }
        }


        return rString;
        */
    }


    public String getResults(){


        // to adapt to the "old" structure of components

        Map<String, List<Node>> myResults = new HashMap<String,List<Node>>();

        for(Node n:components.keySet()){
            if(!myResults.containsKey(components.get(n))){
                ArrayList<Node> newList = new ArrayList<>();
                newList.add(n);
                myResults.put(components.get(n),newList);
            } else{
                List<Node> oldList = myResults.get(components.get(n));
                oldList.add(n);
                myResults.put(components.get(n),oldList);
            }
        }

        // Building the result string

        StringBuilder returnString = new StringBuilder();
        returnString.append("Component count: " + myResults.keySet().size() + "\n");
        returnString.append("- - - - - - - -\n");
        for(String s:myResults.keySet()){
            boolean first = true;
            returnString.append("Component " + s + ": ");
            for(Node n:myResults.get(s)){
                if(!first){
                    returnString.append(", ");
                } else{
                    first = false;
                }
                returnString.append(n.getId());
            }
            returnString.append("\n");
        }

        returnString.append("- - - - - - - -\n");
        returnString.append("Done in: " + timer.elapsed(TimeUnit.MICROSECONDS)+ "\u00B5s");

        return returnString.toString();
    }

}
