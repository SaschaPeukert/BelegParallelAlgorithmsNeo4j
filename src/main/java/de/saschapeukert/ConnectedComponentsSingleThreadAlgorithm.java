package de.saschapeukert;

import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sascha Peukert on 06.08.2015.
 */
public class ConnectedComponentsSingleThreadAlgorithm extends AlgorithmRunnable {


    public enum AlgorithmType{
        WEAK,
        STRONG
    }


    private Map<Node,String> components_weak;
    private Map<String, List<Node>> components_strong;
    private List<Node> doneNodes;

    private AlgorithmType myType;


    public ConnectedComponentsSingleThreadAlgorithm(GraphDatabaseService gdb, AlgorithmType type){
        super(gdb);
        components_weak = new HashMap<Node, String>();
        components_strong = new TreeMap<String,List<Node>>();
        this.myType = type;
        this.doneNodes = new ArrayList<Node>();

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
                if(components_weak.get(n) == null){
                    if(myType==AlgorithmType.WEAK){
                        DFS_weak(n, "C" + componentID);
                    } else{
                        ArrayList<Node> newList = new ArrayList<>(); // initialize every list
                        components_strong.put("C"+componentID,newList);
                        DFS_strong(n, "C" + componentID, true);
                    }

                    componentID++;
                }

            }

            tx.success();
        }

        timer.stop();


    }

    private void DFS_strong(Node n, String compName, boolean both){

        List<Node> list = components_strong.get(compName);


        if(doneNodes.contains(n)){
            return;
        }
        /*
        boolean contains = false;

        for(Node q: list){
            if(n.equals(q)){
                contains = true;
            }
        }

        if(contains){
            return; // Already visited
        }*/

        // NOW IT HAS TO BE NULL
        list.add(n);
        components_strong.put(compName, list);

        doneNodes.add(n);
        if(both){
            for(Relationship r :n.getRelationships(Direction.OUTGOING)) {
                boolean newNode = true;
                for(String s: components_strong.keySet()) {
                    if((components_strong.get(s).contains(r.getOtherNode(n)) && (components_strong.get(s).contains(n)))){
                        newNode = false;
                    }
                }
                if(newNode)
                    DFS_strong(r.getOtherNode(n), compName, true);
            }
        }
        for(Relationship r :n.getRelationships(Direction.INCOMING)) {
            DFS_strong(r.getOtherNode(n), compName,false);
        }

    }



    private void DFS_weak(Node n, String compName){
        //String rString = "";

        if(components_weak.get(n)==compName){
            return;// rString; // Already visited
        }
        /*
        if(components_weak.get(n)!=null){
            return;// components_weak.get(n);  // THIS WILL NEVER HAPPEN
        }
        */
        // NOW IT HAS TO BE NULL
        components_weak.put(n, compName);

        for(Relationship r :n.getRelationships()){
            DFS_weak(r.getOtherNode(n), compName);

        }
        /*
        // USELESS EXPERIMENTAL STUFF
        String s = "Lets go";
        while(s!=""){
            s="";
            for(Relationship r :n.getRelationships()){
                s = DFS(r.getOtherNode(n),compName);

                if(s!=""){
                    components_weak.put(n,s);
                    break;
                }
            }
        }


        return rString;
        */
    }


    public String getResults(){

        Map<String, List<Node>> myResults = new TreeMap<String,List<Node>>();


        if(myType==AlgorithmType.STRONG){
            /*for(String s:components_strong.keySet()){

                if(components_strong.get(s).size()==0){
                    continue;
                }

                ArrayList<Node> list = new ArrayList<>();
                Node[] obj_nodes = (Node[]) components_strong.get(s).toArray();

                for(Node n:obj_nodes){
                    list.add(n);
                }

                myResults.put(s,list);
            }*/

            myResults = components_strong;

        } else{

            // to adapt to the "old" structure of components_weak

            for(Node n: components_weak.keySet()){
                if(!myResults.containsKey(components_weak.get(n))){
                    ArrayList<Node> newList = new ArrayList<>();
                    newList.add(n);
                    myResults.put(components_weak.get(n),newList);
                } else{
                    List<Node> oldList = myResults.get(components_weak.get(n));
                    oldList.add(n);
                    myResults.put(components_weak.get(n),oldList);
                }
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
