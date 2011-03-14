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
package org.neo4j.bench.cases.mixedload;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.bench.cases.mixedload.workers.BulkCreateWorker;
import org.neo4j.bench.cases.mixedload.workers.BulkReaderWorker;
import org.neo4j.bench.cases.mixedload.workers.CreateWorker;
import org.neo4j.bench.cases.mixedload.workers.DeleteWorker;
import org.neo4j.bench.cases.mixedload.workers.PropertyAddWorker;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

/**
 * The main driver for the operation performer threads. Keeps the probabilities
 * with which each thread type is launched and aggregates the results of their
 * runs.
 *
 * @author <a href=mailto:chris.gioran@neotechnology.com> Chris Gioran </a>
 *
 */
public class MixedLoadBenchCase
{
    private enum WorkerType
    {
        SIMPLE,
        BULK
    }

    // Keeps the list of simple workers futures
    private final List<Future<int[]>> simpleTasks;
    // Keeps the list of bulk workers futures
    private final List<Future<int[]>> bulkTasks;
    private int tasksExecuted;
    // The queue of nodes created/deleted
    private final LinkedBlockingQueue<Node> nodes;

    private long totalReads = 0;
    private long totalWrites = 0;
    private long totalTime = 0;
    // Current max values
    private double peakReads = 0;
    private double peakWrites = 0;
    private double sustainedReads = 0;
    private double sustainedWrites = 0;
    // Time to run, in minutes
    private final long timeToRun;

    public MixedLoadBenchCase( long timeToRun )
    {
        simpleTasks = new LinkedList<Future<int[]>>();
        bulkTasks = new LinkedList<Future<int[]>>();
        this.timeToRun = timeToRun;
        nodes = new LinkedBlockingQueue<Node>();
        tasksExecuted = 0;
    }

    public double[] getResults()
    {
        double[] totals = new double[6];
        totals[0] = totalReads * 1.0 / totalTime;
        totals[1] = totalWrites * 1.0 / totalTime;
        totals[2] = peakReads;
        totals[3] = peakWrites;
        totals[4] = sustainedReads;
        totals[5] = sustainedWrites;
        return Arrays.copyOf( totals, totals.length );
    }

    public Queue<Node> getNodeQueue()
    {
        return nodes;
    }

    public void run( GraphDatabaseService graphDb )
    {

        int maxThreads = Runtime.getRuntime().availableProcessors() + 2;
        ExecutorService service = Executors.newFixedThreadPool( maxThreads );
        Random r = new Random();

        long startTime = System.currentTimeMillis();
        try
        {
            service.submit( new BulkCreateWorker( graphDb, nodes, 10000 ) ).get();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }

        while ( System.currentTimeMillis() - startTime < timeToRun * 60 * 1000 )
        {
            double dice = r.nextDouble();
            if ( dice > 0.5 )
            {
                simpleTasks.add( service.submit( new CreateWorker( graphDb,
                        nodes, r.nextInt( 1000 ) ) ) );
            }
            else if ( dice > 0.1 )
            {
                simpleTasks.add( service.submit( new DeleteWorker( graphDb,
                        nodes, r.nextInt( 1000 ) ) ) );
            }
            if ( r.nextDouble() > 0.3 )
            {
                bulkTasks.add( service.submit( new BulkCreateWorker( graphDb,
                        nodes, 2000 ) ) );
            }
            if ( r.nextDouble() < 0.1 )
            {
                bulkTasks.add( service.submit( new BulkReaderWorker( graphDb ) ) );
            }
            if ( r.nextBoolean() )
            {
                simpleTasks.add( service.submit( new PropertyAddWorker(
                        graphDb, nodes, 200 ) ) );
            }
            try
            {
                while ( simpleTasks.size() + bulkTasks.size() > maxThreads - 2 )
                {
                    gatherUp( simpleTasks, WorkerType.SIMPLE, false );
                    gatherUp( bulkTasks, WorkerType.BULK, false );
                }
                printOutResults( "Intermediate results" );
            }
            catch ( InterruptedException e )
            {
                e.printStackTrace();
            }
        }
        try
        {
            getAll();
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
        }
        printOutResults( "Final results" );
        System.out.println( "Run for "
                            + ( System.currentTimeMillis() - startTime )
                            / 60000 + " minutes" );
        service.shutdown();
    }
    
    private void printOutResults(String header)
    {
        System.out.println( header );
        System.out.println( "Total time (ms): " + totalTime );
        System.out.println( "Total reads: " + totalReads );
        System.out.println( "Total writes: " + totalWrites );
        System.out.println( "Peak reads per ms:" + peakReads );
        System.out.println( "Peak writes per ms: " + peakWrites );
        System.out.println( "Peak Sustained reads per ms: " + sustainedReads );
        System.out.println( "Peak Sustained writes per ms: " + sustainedWrites );
        System.out.println();
    }

    /**
     * 
     * @param tasks The list of Futures to gather
     * @param type The type of tasks - used for statistics generation
     * @param sweepUp True if this method should wait for unfinished tasks if
     *            false, not done tasks are skipped
     * @throws InterruptedException
     */
    private void gatherUp( List<Future<int[]>> tasks, WorkerType type,
            boolean sweepUp ) throws InterruptedException
    {
        Iterator<Future<int[]>> it = tasks.iterator();
        while ( it.hasNext() )
        {
            Future<int[]> task = it.next();
            // Short circuit - if sweepUp is true, we gather everything up, else
            // only finished
            if ( sweepUp || task.isDone() )
            {
                tasksExecuted++;
                try
                {
                    int[] taskRes = task.get();
                    totalReads += taskRes[0];
                    totalWrites += taskRes[1];
                    totalTime += taskRes[2];
                    // These are the means for this run
                    double thisReads = taskRes[0]
                                       / ( taskRes[2] == 0 ? 1 : taskRes[2] );
                    double thisWrites = taskRes[1]
                                        / ( taskRes[2] == 0 ? 1 : taskRes[2] );
                    // Sustained operations must be at least as long as half the average runtime
                    if ( type == WorkerType.BULK && taskRes[2] > totalTime*0.5/tasksExecuted )
                    {
                        if ( thisReads > sustainedReads )
                            sustainedReads = thisReads;
                        if ( thisWrites > sustainedWrites )
                            sustainedWrites = thisWrites;
                    }
                    // The test run for more than 10% of the average time, long enough for
                    // getting a peak value
                    if ( taskRes[2] > totalTime*0.1/tasksExecuted )
                    {
                        if ( thisReads > peakReads ) peakReads = thisReads;
                        if ( thisWrites > peakWrites ) peakWrites = thisWrites;
                    }
                }
                catch ( ExecutionException e )
                {
                    // It threw an exception, print and continue
                    e.printStackTrace();
                    continue;
                }
                finally
                {
                    it.remove();
                }

            }
        }
    }

    private void getAll() throws InterruptedException
    {
        gatherUp( simpleTasks, WorkerType.SIMPLE, true );
        gatherUp( bulkTasks, WorkerType.BULK, true );
    }
}
