/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bench.cases.mixedload.workers;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class BulkCreateWorker implements Callable<int[]>
{

    private enum RelType implements RelationshipType
    {
        TYPE_BULK
    }

    private final GraphDatabaseService graphDb;
    private final Queue<Node> nodes;
    private final Random r;
    private int ops;

    private int reads;
    private int writes;

    public BulkCreateWorker( GraphDatabaseService graphDb, Queue<Node> nodes, int ops )
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
        List<Node> myNodes = new LinkedList<Node>();
        int[] result = new int[3];
        long time = System.currentTimeMillis();

        Transaction tx = graphDb.beginTx();
        try
        {
            while ( ops-- > 0 )
            {
                if ( myNodes.size() < 4 || r.nextDouble() < 0.75 )
                {
                    myNodes.add( graphDb.createNode() );
                    writes += 1;
                }
                else
                {
                    createRandomRelationship( myNodes );
                }
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
        result[0] = reads;
        result[1] = writes;
        result[2] = (int) (System.currentTimeMillis() - time);
        nodes.addAll( myNodes );
        return result;
    }

    private void createRandomRelationship( List<Node> myNodes )
    {
        int one, two;
        do
        {
            one = r.nextInt( myNodes.size() );
            two = r.nextInt( myNodes.size() );
        }
        while ( one == two );

        Node from, to;

        from = myNodes.get( one );
        to = myNodes.get( two );

        if ( r.nextBoolean() )
        {
            from.createRelationshipTo( to, RelType.TYPE_BULK );
        }
        else
        {
            to.createRelationshipTo( from, RelType.TYPE_BULK );
        }
        reads += 2; // For the nodes
        writes += 1; // For the relationship
    }
}
