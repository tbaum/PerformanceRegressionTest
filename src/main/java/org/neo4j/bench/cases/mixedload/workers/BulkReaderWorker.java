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
        for (int i = 0; i < 10; i++)
        {
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
        }

        int[] result = new int[3];
        result[0] = reads;
        result[1] = writes;
        result[2] = (int) (System.currentTimeMillis() - time);
        return result;
    }
}
