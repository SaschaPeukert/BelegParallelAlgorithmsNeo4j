package de.saschapeukert.pluginStuff;

import org.neo4j.graphdb.GraphDatabaseService;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.nio.charset.Charset;

/*
    REMEMBER: THE PACKAGE HAS TO BE CORRECTLY NAMED IN THE SERVER.CONF

 */


//START SNIPPET: HelloWorldResource
@Path( "/helloworld" )
public class HelloWorldResource
{
    private final GraphDatabaseService database;

    public HelloWorldResource( @Context GraphDatabaseService database )
    {
        this.database = database;
    }

    @GET
    @Produces( MediaType.TEXT_PLAIN )
    @Path( "/{nodeId}" )
    public Response hello( @PathParam( "nodeId" ) long nodeId )
    {
        // Do pluginStuff with the database
        return Response.status( Status.OK ).entity(
                ("Hello World, nodeId=" + nodeId).getBytes( Charset.forName("UTF-8") ) ).build();
    }
}