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
public class ExtensionRandomWalk
{
    private DBUtils db;

    public ExtensionRandomWalk(@Context GraphDatabaseService database )
    {
        db = DBUtils.getInstance(database);
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
        refreshHighestIds(); // this is needed!
        long[] times = Starter.mainAsExtension(argsRW);

        return "Random Walk done in " + times[0] + "ms.\n +" +
                        "Writing done in "+ times[1] +"ms.";
    }

    private void refreshHighestIds(){
        db.refreshHighestNodeID();
        db.refreshHighestRelationshipID();
    }
}