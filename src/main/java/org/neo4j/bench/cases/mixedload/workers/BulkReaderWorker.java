
package org.neo4j.bench.cases.mixedload.workers;

import java.util.concurrent.Callable;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public class BulkReaderWorker implements Callable<int[]>
{

    private final GraphDatabaseService graphDb;

    private int reads;
    private int writes;

    public BulkReaderWorker( GraphDatabaseService graphDb )
    {
        this.graphDb = graphDb;

        this.reads = 0;
        this.writes = 0;
    }

    @Override
    public int[] call() throws Exception
    {
        long time = System.currentTimeMillis();
        for ( Node node : graphDb.getAllNodes() )
        {
            reads += 1;
            for ( Relationship r : node.getRelationships() )
            {
                reads += 1;
                for (String propertyKey : r.getPropertyKeys())
                {
                    r.getProperty( propertyKey );
                    reads += 2; // Prop key and prop value
                }
            }
            for (String propertyKey : node.getPropertyKeys())
            {
                node.getProperty( propertyKey );
                reads += 2; // Prop key and prop value
            }
        }

        int[] result = new int[3];
        result[0] = reads;
        result[1] = writes;
        result[2] = (int) (System.currentTimeMillis() - time);
        return result;
    }
}
