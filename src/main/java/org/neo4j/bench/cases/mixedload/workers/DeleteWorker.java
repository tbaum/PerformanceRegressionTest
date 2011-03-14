package org.neo4j.bench.cases.mixedload.workers;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class DeleteWorker implements Callable<int[]>
{

    private final GraphDatabaseService graphDb;
    private final Queue<Node> nodes;
    private final Random r;
    private int ops;

    private int reads;
    private int writes;

    public DeleteWorker( GraphDatabaseService graphDb, Queue<Node> nodes,
            int ops )
    {
        this.graphDb = graphDb;
        this.nodes = nodes;
        this.r = new Random();
        this.ops = ops;
        this.reads = 0;
        this.writes = 0;
    }

    @Override
    public int[] call() throws Exception
    {
        long time = System.currentTimeMillis();
        while ( ops-- > 0 )
        {
            Transaction tx = graphDb.beginTx();
            try
            {
                if ( r.nextDouble() > 0.4 )
                {
                    deleteRandomNode();
                }
                else
                {
                    deleteRandomRelationship();
                }
                tx.success();
            }
            catch ( Exception e )
            {
                tx.failure();
                throw e;
            }
            finally
            {
                tx.finish();
            }
        }
        int[] result = new int[3];
        result[0] = reads;
        result[1] = writes;
        result[2] = (int) (System.currentTimeMillis() - time);
        return result;
    }

    private void deleteRandomNode()
    {
        int delIndex = r.nextInt( nodes.size() );
        Node toDelete;

        for ( int i = 0; i < delIndex; i++ )
        {
            nodes.add( nodes.remove() );
        }
        toDelete = nodes.remove();
        for ( Relationship rel : toDelete.getRelationships( Direction.BOTH ) )
        {
            rel.delete();
            writes += 1; // The relationship delete
            reads += 1;
        }
        nodes.remove( toDelete );
        graphDb.getNodeById( toDelete.getId() ).delete();
        reads += 1; // The node read in
        writes += 1; // The node delete
    }

    private void deleteRandomRelationship()
    {
        // Relationship toDelete;
        // int delIndex = r.nextInt( rels.size() );
        // Iterator<Relationship> it = rels.iterator();
        //
        // for ( int i = 0; i < delIndex; i++ )
        // {
        // it.next();
        // }
        // toDelete = it.next();
        // }
        // graphDb.getRelationshipById( toDelete.getId() ).delete();
        // writes += 1; // The relationship delete
        // reads += 1;
    }

}
