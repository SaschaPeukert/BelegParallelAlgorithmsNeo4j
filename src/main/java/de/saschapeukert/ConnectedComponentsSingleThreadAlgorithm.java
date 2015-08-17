package de.saschapeukert;

import org.neo4j.graphdb.*;

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

    private Map<Node,TarjanNode> nodeDictionary;

    private Stack<Node> stack;
    private int maxdfs=0;
    private int componentID;

    private AlgorithmType myType;


    public ConnectedComponentsSingleThreadAlgorithm(GraphDatabaseService gdb, Set<Node> nodes, AlgorithmType type){
        super(gdb, nodes);
        components_weak = new HashMap<Node, String>();
        this.myType = type;

        if(myType==AlgorithmType.STRONG){
            this.stack = new Stack<Node>();
            this.nodeDictionary = new HashMap<Node,TarjanNode>();

            // initialize nodeDictionary

            Iterator<Node> it = nodes.iterator();
            while(it.hasNext()){
                Node n = it.next();
                nodeDictionary.put(n,new TarjanNode(n));
            }
        }

    }


    @Override
    public void compute() {

        timer.start();

        componentID = 0;


        try (Transaction tx = graphDb.beginTx()) {
//            GlobalGraphOperations operations = GlobalGraphOperations.at(graphDb);
//            ResourceIterable<Node> it = operations.getAllNodes();

            //for (Node n : it) {
            while(allNodes.size()!=0){
                // Every node has to be marked as (part of) a component
                Node n = (Node) allNodes.toArray()[0]; // TODO: Better Way?
                // x.toArray (new Foo[x.size ()])

                //if(components_weak.get(n) == null){  // useless?!
                    if(myType==AlgorithmType.WEAK){
                        DFS(n, "C" + componentID);
                        componentID++;
                    } else{
                        tarjan(n);
                    }


                //}

            }

            tx.success();
        }

        timer.stop();


    }

    private void tarjan(Node currentNode){

        TarjanNode v = nodeDictionary.get(currentNode);
        v.dfs = maxdfs;
        v.lowlink = maxdfs;
        maxdfs++;

        v.onStack = true;           // This should be atomic
        stack.push(currentNode);        // !

        allNodes.remove(currentNode);

        Iterable<Relationship> it = currentNode.getRelationships(Direction.OUTGOING);
        for(Relationship r: it){
            Node n_new = r.getOtherNode(currentNode);
            TarjanNode v_new = nodeDictionary.get(n_new);

            if(currentNode.getId()==345){
                System.out.println("Hallo!");
            }

            if(allNodes.contains(n_new)){
                tarjan(n_new);

                v.lowlink = Math.min(v.lowlink,v_new.lowlink);

            } else if(v_new.onStack){       // O(1)

                v.lowlink = Math.min(v.lowlink,v_new.dfs);
            }

        }

        if(v.lowlink == v.dfs){
            // Root of a SCC
            //System.out.print("\nSZK: ");
            while(true){
                Node node_v = stack.pop();
                TarjanNode v_new = nodeDictionary.get(node_v);
                v_new.onStack= false;

                //System.out.print(node_v.getId() + " ");

                components_weak.put(node_v,"C"+componentID);
                if(node_v.getId()== currentNode.getId()){
                    componentID++;
                    break;
                }

            }
        }

        //


    }


    private void DFS(Node n, String compName){
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
        allNodes.remove(n); // correct?

        for(Relationship r :n.getRelationships()){
            DFS(r.getOtherNode(n), compName);

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

        // Building the result string

        StringBuilder returnString = new StringBuilder();
        returnString.append("Component count: " + myResults.keySet().size() + "\n");
        returnString.append("Components with Size >1\n");
        returnString.append("- - - - - - - -\n");
        for(String s:myResults.keySet()){
            if(myResults.get(s).size()<=1){
                continue;
            }

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
