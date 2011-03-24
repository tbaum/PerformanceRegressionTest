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
package org.neo4j.bench.regression.main;

import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.neo4j.bench.cases.mixedload.MixedLoadBenchCase;
import org.neo4j.bench.cases.mixedload.Stats;
import org.neo4j.bench.chart.GenerateOpsPerSecChart;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.EmbeddedGraphDatabase;

@SuppressWarnings( "restriction" )
public class Main
{
    public static void main( String[] args ) throws Exception
    {
        Args argz = new Args( args );
        long timeToRun = Long.parseLong( argz.get( "time-to-run", "120" ) ); // Time
                                                                           // in
                                                                           // minutes
        final GraphDatabaseService db = new EmbeddedGraphDatabase( "db" );
        final MixedLoadBenchCase myCase = new MixedLoadBenchCase( timeToRun );

        myCase.run( db );
        db.shutdown();
        double[] results = myCase.getResults();
        Stats newStats = new Stats(
                new SimpleDateFormat( "MM-dd-HH-mm" ).format( new Date() ) );
        newStats.setAvgReadsPerSec( results[0] );
        newStats.setAvgWritePerSec( results[1] );
        newStats.setPeakReadsPerSec( results[2] );
        newStats.setPeakWritesPerSec( results[3] );
        newStats.setSustainedReadsPerSec( results[4] );
        newStats.setSustainedWritesPerSec( results[5] );

        String statsFilename = argz.get(
                GenerateOpsPerSecChart.OPS_PER_SECOND_FILE_ARG,
                "ops-per-second" );
        String chartFilename = argz.get( GenerateOpsPerSecChart.CHART_FILE_ARG,
                "chart.png" );
        double threshold = Double.parseDouble( argz.get( "threshold", "0.05" ) );

        PrintStream opsPerSecOutFile = new PrintStream( new FileOutputStream(
                statsFilename, true ) );

        newStats.write( opsPerSecOutFile, true );

        GenerateOpsPerSecChart aggreegator = new GenerateOpsPerSecChart(
                statsFilename, chartFilename, threshold );
        aggreegator.process();
    }
}
