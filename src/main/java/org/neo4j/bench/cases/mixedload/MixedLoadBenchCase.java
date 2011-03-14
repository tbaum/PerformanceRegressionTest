package org.neo4j.bench.cases.mixedload;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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

public class MixedLoadBenchCase
{

    private final List<Future<int[]>> tasks;
    private double[] totals;
    private double peakReads = 0;
    private double peakWrites = 0;
    private final long timeToRun;

    public MixedLoadBenchCase( long timeToRun )
    {
        tasks = new LinkedList<Future<int[]>>();
        totals = new double[4];
        totals[0] = 0;
        totals[1] = 0;
        this.timeToRun = timeToRun;
    }

    public double[] getResults()
    {
        totals[2] = peakReads;
        totals[3] = peakWrites;
        return Arrays.copyOf( totals, 4 );
    }

    public void run( GraphDatabaseService graphDb )
    {
        LinkedBlockingQueue<Node> nodes = new LinkedBlockingQueue<Node>();
        int maxThreads = Runtime.getRuntime().availableProcessors() + 2;
        ExecutorService service = Executors.newFixedThreadPool( maxThreads );
        Random r = new Random();

        long startTime = System.currentTimeMillis();
        tasks.add( service.submit( new BulkCreateWorker( graphDb, nodes, 1000 ) ) );
        try
        {
            Thread.sleep( 1000 );
        }
        catch ( InterruptedException e1 )
        {
            e1.printStackTrace();
        }

        while ( System.currentTimeMillis() - startTime < timeToRun * 60 * 1000 )
        {
            double dice = r.nextDouble();
            if ( dice > 0.5 )
            {
                tasks.add( service.submit( new CreateWorker( graphDb, nodes,
                        300 ) ) );
            }
            else if ( dice > 0.1 )
            {
                tasks.add( service.submit( new DeleteWorker( graphDb, nodes, 20 ) ) );
            }
            else
            {
                tasks.add( service.submit( new BulkCreateWorker( graphDb,
                        nodes, 40 ) ) );
            }
            if ( r.nextDouble() < 0.1 )
            {
                tasks.add( service.submit( new BulkReaderWorker( graphDb ) ) );
            }
            if ( r.nextBoolean() )
            {
                tasks.add( service.submit( new PropertyAddWorker( graphDb,
                        nodes, 20 ) ) );
            }
            try
            {
                while ( tasks.size() > maxThreads*2 / 3 )
                {
                    getFinished();
                }
                System.out.println( "Intermediate results:" );
                System.out.println( "Total reads: " + totals[0] );
                System.out.println( "Total writes: " + totals[1] );
                System.out.println( "Total time: " + totals[2] );
                System.out.println( "Peak reads:" + peakReads );
                System.out.println( "Peak writes: " + peakWrites );
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
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println( "Total reads: " + totals[0] );
        System.out.println( "Total writes: " + totals[1] );
        System.out.println( "Total time: " + totals[2] );
        System.out.println( "Peak reads:" + peakReads );
        System.out.println( "Peak writes: " + peakWrites );
        System.out.println( "Run for "
                            + ( System.currentTimeMillis() - startTime )
                            / 60000 + " minutes" );
        service.shutdown();
    }

    private void getFinished() throws InterruptedException
    {
        Iterator<Future<int[]>> it = tasks.iterator();
        while ( it.hasNext() )
        {
            Future<int[]> task = it.next();
            if ( task.isDone() )
            {
                try
                {
                    int[] taskRes = task.get();
                    totals[0] += taskRes[0];
                    totals[1] += taskRes[1];
                    totals[2] += taskRes[2];
                    double thisReads = taskRes[0] * 1000.0 / taskRes[2];
                    double thisWrites = taskRes[1] * 1000.0 / taskRes[2];
                    if ( thisReads > peakReads ) peakReads = thisReads;
                    if ( thisWrites > peakWrites ) peakWrites = thisWrites;
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
        Iterator<Future<int[]>> it = tasks.iterator();
        while ( it.hasNext() )
        {
            Future<int[]> task = it.next();
            try
            {
                int[] taskRes = task.get();
                totals[0] += taskRes[0];
                totals[1] += taskRes[1];
                totals[2] += taskRes[2];
                double thisReads = taskRes[0] * 1000.0 / taskRes[2];
                double thisWrites = taskRes[1] * 1000.0 / taskRes[2];
                if ( thisReads > peakReads ) peakReads = thisReads;
                if ( thisWrites > peakWrites ) peakWrites = thisWrites;
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
