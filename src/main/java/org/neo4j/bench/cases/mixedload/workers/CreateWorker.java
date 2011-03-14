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

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class CreateWorker implements Callable<int[]>
{

    private enum RelType implements RelationshipType
    {
        TYPE_GENERIC
    }

    private final GraphDatabaseService graphDb;
    private final Queue<Node> nodes;
    private final Random r;
    private int ops;

    private int reads;
    private int writes;

    public CreateWorker( GraphDatabaseService graphDb, Queue<Node> nodes, int ops )
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
                if ( r.nextDouble() < 0.75 || nodes.size() < 4 )
                {
                    createNode();
                }
                else
                {
                    createRandomRelationship();
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

    private void createNode()
    {
        nodes.add( graphDb.createNode() );
        writes += 1; // The node
    }

    private void createRandomRelationship()
    {
        int one, two;
        do
        {
            one = r.nextInt( nodes.size() );
            two = r.nextInt( nodes.size() );
        }
        while ( one == two );

        Node from, to;

        int nextStop = Math.min( one, two );
        int i = 0;
        for ( ; i < nextStop; i++ )
        {
            nodes.add( nodes.remove() );
        }
        from = nodes.remove();
        nextStop = Math.max( one, two );
        for ( ; i < nextStop; i++ )
        {
            nodes.add( nodes.remove() );
        }
        to = nodes.remove();

        if ( r.nextBoolean() )
        {
            graphDb.getNodeById( from.getId() ).createRelationshipTo(
                    graphDb.getNodeById( to.getId() ), RelType.TYPE_GENERIC );
        }
        else
        {
            graphDb.getNodeById( to.getId() ).createRelationshipTo(
                    graphDb.getNodeById( from.getId() ), RelType.TYPE_GENERIC );
        }
        reads += 2; // For the nodes
        writes += 1; // For the relationship
    }

}
