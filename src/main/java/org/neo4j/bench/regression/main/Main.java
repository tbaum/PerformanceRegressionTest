package org.neo4j.bench.regression.main;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.neo4j.bench.cases.mixedload.MixedLoadBenchCase;
import org.neo4j.bench.cases.mixedload.Stats;
import org.neo4j.bench.chart.GenerateOpsPerSecChart;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import sun.misc.Signal;
import sun.misc.SignalHandler;

@SuppressWarnings( "restriction" )
public class Main
{
    public static void main( String[] args ) throws Exception
    {
        Args argz = new Args(args);
        long timeToRun = Long.parseLong( argz.get( "time-to-run", "1" ) ); // Time in minutes
        final GraphDatabaseService db = new EmbeddedGraphDatabase( "db" );
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
        MixedLoadBenchCase myCase = new MixedLoadBenchCase(timeToRun);
        myCase.run( db );

        db.shutdown();
        double[] results = myCase.getResults();
        Stats newStats = new Stats(new SimpleDateFormat( "MM-dd-HH-mm" ).format( new Date() ));
        newStats.setAvgReadsPerSec( results[0] );
        newStats.setAvgWritePerSec( results[1] );
        newStats.setPeakReadsPerSec( results[2] );
        newStats.setPeakWritesPerSec( results[3] );
        
        String statsFilename = argz.get( GenerateOpsPerSecChart.OPS_PER_SECOND_FILE_ARG, "ops-per-second" );
        String chartFilename = argz.get( GenerateOpsPerSecChart.CHART_FILE_ARG, "chart.png" );
        double threshold = Double.parseDouble( argz.get( "threshold", "0.05" ) );
        
        PrintStream opsPerSecOutFile = new PrintStream( new FileOutputStream(
                statsFilename, true ) );
        
        newStats.write( opsPerSecOutFile, true );

        GenerateOpsPerSecChart aggreegator = new GenerateOpsPerSecChart( statsFilename, chartFilename, threshold );
        aggreegator.process();
    }
}
