package de.saschapeukert.extension;

import de.saschapeukert.Database.DBUtils;
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
public class ExtensionWarmup
{
    private DBUtils db;

    public ExtensionWarmup(@Context GraphDatabaseService database )
    {
        DBUtils db = DBUtils.getInstance(database);
    }

    @GET
    //@Produces( MediaType.TEXT_PLAIN )
    @Path( "/warmup" )
    public String warmup(@Context GraphDatabaseService database)
    {

        refreshHighestIds(db);
        Starter.SkippingPagesWarmUp(true);

        return  "Warmup done ";
        //return Response.status( Status.OK ).entity(
        //        ("Warmup done").getBytes( Charset.forName("UTF-8") ) ).build();
    }

    @GET
    @Path( "/randomWalk" )
    public String randomWalk( @PathParam( "propertyName" ) String propertyName,
                              @PathParam( "batchsize" ) String batchsize,
                              @PathParam( "number_of_threads" ) String threads,
                              @PathParam( "writeBatchsize" ) String writeBatchsize,
                              @PathParam( "number_of_steps" ) String number_of_steps,
                              @PathParam( "kernelAPI" ) String kernelAPI )
    {

        String[] argsRW = {"RW",propertyName,batchsize,threads,writeBatchsize,number_of_steps,kernelAPI};
        refreshHighestIds(db); // this is needed!
        long[] times = Starter.mainAsExtension(argsRW);

        return "Random Walk done in " + times[0] + "ms.\n +" +
                "Writing done in "+ times[1] +"ms.";
    }

    @GET
    @Path( "/weaklyConnectedComponents" )
    public String wcc( @PathParam( "propertyName" ) String propertyName,
                              @PathParam( "batchsize" ) String batchsize,
                              @PathParam( "number_of_threads" ) String threads,
                              @PathParam( "writeBatchsize" ) String writeBatchsize)
    {

        String[] argsWCC = {"WCC",propertyName,batchsize,threads,writeBatchsize};
        refreshHighestIds(db); // this is needed!
        long[] times = Starter.mainAsExtension(argsWCC);

        return "Weakly Connected Components done in " + times[0] + "ms.\n +" +
                "Writing done in "+ times[1] +"ms.";
    }

    @GET
    @Path( "/stronglyConnectedComponents" )
    public String scc( @PathParam( "propertyName" ) String propertyName,
                       @PathParam( "batchsize" ) String batchsize,
                       @PathParam( "number_of_threads" ) String threads,
                       @PathParam( "writeBatchsize" ) String writeBatchsize)
    {

        String[] argsSCC = {"SCC",propertyName,batchsize,threads,writeBatchsize};
        refreshHighestIds(db); // this is needed!
        long[] times = Starter.mainAsExtension(argsSCC);

        return "Strongly Connected Components done in " + times[0] + "ms.\n +" +
                "Writing done in "+ times[1] +"ms.";
    }

    private void refreshHighestIds(DBUtils db){
        db.refreshHighestNodeID();
        db.refreshHighestRelationshipID();
    }
}