package de.saschapeukert.extension;

import de.saschapeukert.database.DBUtils;
import de.saschapeukert.Starter;
import org.neo4j.graphdb.GraphDatabaseService;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

/*
    REMEMBER: THE PACKAGE HAS TO BE CORRECTLY NAMED IN THE SERVER.CONF

 */

@Path( "/algorithms" )
public class Extension
{
    private DBUtils db;

    public Extension(@Context GraphDatabaseService database )
    {
         db = new DBUtils(database);
    }

    @GET
    @Path( "/warmup" )
    public String warmup()
    {
        refreshHighestIds(db);
        Starter.SkippingPagesWarmUp(true, db);

        return  "Warmup done ";
    }

    @GET
    @Path( "/randomWalk/{propertyName}/{batchsize}/{number_of_threads}/{writeBatchsize}/" +
            "{number_of_steps}/{kernelAPI}" )
    public String randomWalk( @PathParam( "propertyName" ) String propertyName,
                              @PathParam( "batchsize" ) String batchsize,
                              @PathParam( "number_of_threads" ) String threads,
                              @PathParam( "writeBatchsize" ) String writeBatchsize,
                              @PathParam( "number_of_steps" ) String number_of_steps,
                              @PathParam( "kernelAPI" ) String kernelAPI )
    {

        String[] argsRW = {"RW",propertyName,batchsize,threads,writeBatchsize,number_of_steps,kernelAPI};
        refreshHighestIds(db); // this is needed!
        long[] times = Starter.mainAsExtension(argsRW,db);

        return "Random Walk done in " + times[0] + "ms.\n" +
                "Writing done in "+ times[1] +"ms.";
    }

    @GET
    @Path( "/weaklyConnectedComponents/{propertyName}/{batchsize}/{number_of_threads}/{writeBatchsize}")
            public String wcc( @PathParam( "propertyName" ) String propertyName,
                              @PathParam( "batchsize" ) String batchsize,
                              @PathParam( "number_of_threads" ) String threads,
                              @PathParam( "writeBatchsize" ) String writeBatchsize)
    {

        String[] argsWCC = {"WCC",propertyName,batchsize,threads,writeBatchsize};
        refreshHighestIds(db); // this is needed!
        long[] times = Starter.mainAsExtension(argsWCC, db);

        return "Weakly Connected Components done in " + times[0] + "ms.\n" +
                "Writing done in "+ times[1] +"ms.";
    }

    @GET
    @Path( "/stronglyConnectedComponents/{propertyName}/{batchsize}/{number_of_threads}/{writeBatchsize}" )
    public String scc( @PathParam( "propertyName" ) String propertyName,
                       @PathParam( "batchsize" ) String batchsize,
                       @PathParam( "number_of_threads" ) String threads,
                       @PathParam( "writeBatchsize" ) String writeBatchsize)
    {

        String[] argsSCC = {"SCC",propertyName,batchsize,threads,writeBatchsize};
        refreshHighestIds(db); // this is needed!
        long[] times = Starter.mainAsExtension(argsSCC, db);

        return "Strongly Connected Components done in " + times[0] + "ms.\n" +
                "Writing done in "+ times[1] +"ms.";
    }

    private void refreshHighestIds(DBUtils db){
        db.refreshHighestNodeID();
        db.refreshHighestRelationshipID();
    }
}