package org.neo4j.bench.regression.main;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.neo4j.bench.cases.mixedload.MixedLoadBenchCase;
import org.neo4j.bench.chart.GenerateOpsPerSecChart;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import sun.misc.Signal;
import sun.misc.SignalHandler;

@SuppressWarnings( "restriction" )
public class Main
{
    public static void main( String[] args ) throws Exception
    {
        final GraphDatabaseService db = new EmbeddedGraphDatabase( "foo" );
        SignalHandler handler = new SignalHandler()
        {
            @Override
            public void handle( Signal arg0 )
            {
                int count = 0;
                for (Node n : db.getAllNodes())
                {
                    count++;
                }
                System.out.println("There are "+count+" nodes in the db");
            }
        };
        /*
         * SIGUSR1 is used by the JVM and INT, ABRT and friends
         * are all defined for specific usage by POSIX. While SIGINT
         * is conveniently issued by Ctrl-C, SIGUSR2 is for user defined
         * behavior so this is what I use.
         */
        Signal signal = new Signal( "USR2" );

        Signal.handle( signal, handler );
        MixedLoadBenchCase myCase = new MixedLoadBenchCase();
        myCase.run( db );

        db.shutdown();
        double[] results = myCase.getResults();
        
        PrintStream opsPerSecOutFile = new PrintStream( new FileOutputStream(
                "ops-per-second", true ) );
        opsPerSecOutFile.println( String.format( "%s\t%.2f\t%.2f\t%.2f\t%.2f",
                new SimpleDateFormat( "MM-dd-HH-mm" ).format( new Date() ),
                results[0], results[1], results[2], results[3]
                ));

        GenerateOpsPerSecChart aggreegator = new GenerateOpsPerSecChart( "ops-per-second", "chart.png", 0.05 );
        aggreegator.process();
    }
}
