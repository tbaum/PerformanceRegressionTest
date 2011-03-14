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
package org.neo4j.bench.chart;

import java.awt.Color;
import java.awt.Dimension;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

public class GenerateOpsPerSecChart
{
    private static final int TESTS_TO_DRAW = 10;
    public static final String OPS_PER_SECOND_FILE = "ops-per-second";

    private String inputFilename;
    private String outputFilename;
    private boolean alarm;
    private SortedMap<String, double[]> data;
    private Map<String, double[]> dataToDraw;
    private double threshold;

    public GenerateOpsPerSecChart( String inputFilename, String outputFilename,
            double threshold )
    {
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
        this.threshold = threshold;
        data = loadOpsPerSecond( this.inputFilename );
    }

    public boolean process() throws Exception
    {
        Set<String> versions = data.keySet();
        // Take the 10 latest
        if ( versions.size() > TESTS_TO_DRAW )
        {
            Iterator<String> it = versions.iterator();
            int i = 0;
            while ( versions.size() - i++ > TESTS_TO_DRAW )
            {
                it.next();
            }
            dataToDraw = data.tailMap( it.next() );
        }
        else
        {
            dataToDraw = data;
        }
        alarm = detectDegradation( threshold ) != null;
        generateChart();
        return alarm;
    }

    private String detectDegradation( double threshold )
    {
        String latestRun = data.lastKey();
        System.out.println( "Latest run test is " + latestRun );
        double[] latestNumbers = data.get( latestRun );
        System.out.println( "With values " + latestNumbers[0] + ", "
                            + latestNumbers[1] );
        for ( Map.Entry<String, double[]> previous : data.headMap( latestRun ).entrySet() )
        {
            double previousReads = previous.getValue()[0];
            double previousWrites = previous.getValue()[1];
            if ( previousReads * ( 1 + threshold ) > latestNumbers[0]
                 || previousWrites * ( 1 + threshold ) > latestNumbers[1] )
            {
                return previous.getKey();
            }
        }
        return null;
    }

    private void generateChart() throws Exception
    {
        DefaultCategoryDataset dataset = generateDataset();
        JFreeChart chart = ChartFactory.createBarChart( "Performance chart",
                "Bench case", "Operations per sec", dataset,
                PlotOrientation.VERTICAL, true, true, false );

        Dimension dimensions = new Dimension( 1600, 900 );
        File chartFile = new File( outputFilename );
        if ( alarm )
        {
            chart.setBackgroundPaint( Color.RED );
        }
        ChartUtilities.saveChartAsPNG( chartFile, chart,
                (int) dimensions.getWidth(), (int) dimensions.getHeight() );
    }

    private DefaultCategoryDataset generateDataset()
    {
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for ( String key : dataToDraw.keySet() )
        {
            double[] rw = dataToDraw.get( key );
            dataset.addValue( rw[0], "reads", key );
            dataset.addValue( rw[1], "writes", key );
            dataset.addValue( rw[2], "peak reads", key );
            dataset.addValue( rw[3], "peak writes", key );
        }
        return dataset;
    }

    /**
     * Opens the operations per second file, reads in the contents and creates a
     * Map from the version name to an array of four doubles, the first being
     * the average read operations per second, the second the write operations
     * per second, the third the peak read operations per second and the fourth
     * the peak write operations per second. If any I/O error occurs, including
     * a non-existent file, it returns null.
     * 
     * @param arguments The arguments as passed in main
     * @return A Map from version to operations per second
     */
    public static SortedMap<String, double[]> loadOpsPerSecond( String fileName )
    {
        File dataFile = new File( fileName );
        if ( !dataFile.exists() )
        {
            return null;
        }
        BufferedReader reader = null;
        SortedMap<String, double[]> result = new TreeMap<String, double[]>();
        try
        {
            reader = new BufferedReader( new FileReader( dataFile ) );
            String line, // The current line
            versionToken, // The current version token
            readsToken, // The current reads per second token
            writesToken, // The current writes per second token
            peakReadsToken, // The current peak reads token
            peakWritesToken // The current peak writes token
            ;
            double reads, writes, peakReads, peakWrites; // The double values of
                                                         // the corresponding
                                                         // tokens
            while ( ( line = reader.readLine() ) != null )
            {
                StringTokenizer tokenizer = new StringTokenizer( line, "\t" );
                if ( tokenizer.countTokens() < 5 )
                {
                    continue;
                }
                // Grab the tokens
                versionToken = tokenizer.nextToken();
                readsToken = tokenizer.nextToken();
                writesToken = tokenizer.nextToken();
                peakReadsToken = tokenizer.nextToken();
                peakWritesToken = tokenizer.nextToken();
                // Parse the integer values, check for validity
                try
                {
                    reads = Double.valueOf( readsToken );
                    writes = Double.valueOf( writesToken );
                    peakReads = Double.valueOf( peakReadsToken );
                    peakWrites = Double.valueOf( peakWritesToken );
                }
                catch ( NumberFormatException e )
                {
                    // This is stupid but there is no other way
                    continue;
                }
                double[] opsStats = new double[4];
                opsStats[0] = reads;
                opsStats[1] = writes;
                opsStats[2] = peakReads;
                opsStats[3] = peakWrites;
                result.put( versionToken, opsStats );
            }
        }
        catch ( IOException e )
        {
            // This should not happen as we check above
            e.printStackTrace();
            return null;
        }
        finally
        {
            if ( reader != null )
            {
                try
                {
                    reader.close();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }
}
