package de.saschapeukert.extension;

import de.saschapeukert.Database.DBUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;

/*
    REMEMBER: THE PACKAGE HAS TO BE CORRECTLY NAMED IN THE SERVER.CONF

 */

@Path( "/warmup" )
public class ExtensionWarmup
{

    public ExtensionWarmup(@Context GraphDatabaseService database )
    {
    }

    @GET
    //@Produces( MediaType.TEXT_PLAIN )
    @Path( "/do" )
    public String warmup(@Context GraphDatabaseService database)
    {

        DBUtils db = DBUtils.getInstance(database);

        refreshHighestIds(db);
        //Starter.SkippingPagesWarmUp(true);
        Transaction tx = db.openTransaction();
        tx.success();

        return  "Warmup done ";
        //return Response.status( Status.OK ).entity(
        //        ("Warmup done").getBytes( Charset.forName("UTF-8") ) ).build();
    }

    private void refreshHighestIds(DBUtils db){
        db.refreshHighestNodeID();
        db.refreshHighestRelationshipID();
    }
}