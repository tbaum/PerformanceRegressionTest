package org.neo4j.bench.cases.mixedload;

import java.io.PrintStream;
import java.util.StringTokenizer;

public class Stats implements Comparable<Stats>
{

    private String name;
    private double avgReadsPerSec;
    private double avgWritePerSec;
    private double peakReadsPerSec;
    private double peakWritesPerSec;
    
    public Stats(String name)
    {
        this.name = name;
    }
    
    public double getAvgReadsPerSec()
    {
        return avgReadsPerSec;
    }

    public void setAvgReadsPerSec( double avgReadsPerSec )
    {
        this.avgReadsPerSec = avgReadsPerSec;
    }

    public double getAvgWritePerSec()
    {
        return avgWritePerSec;
    }

    public void setAvgWritePerSec( double avgWritePerSec )
    {
        this.avgWritePerSec = avgWritePerSec;
    }

    public double getPeakReadsPerSec()
    {
        return peakReadsPerSec;
    }

    public void setPeakReadsPerSec( double peakReadsPerSec )
    {
        this.peakReadsPerSec = peakReadsPerSec;
    }

    public double getPeakWritesPerSec()
    {
        return peakWritesPerSec;
    }

    public void setPeakWritesPerSec( double peakWritesPerSec )
    {
        this.peakWritesPerSec = peakWritesPerSec;
    }

    public String getName()
    {
        return name;
    }
    
    public void write(PrintStream out, boolean newLine)
    {
        out.print( String.format( "%s\t%.2f\t%.2f\t%.2f\t%.2f",
                name,
                avgReadsPerSec, avgWritePerSec, peakReadsPerSec, peakWritesPerSec
                ));
        if (newLine)
        {
            out.println();
        }
    }

    @Override
    public int compareTo( Stats o )
    {
        // NPE on purpose
        return this.name.compareTo( o.name );
    }
    
    public static Stats parse(String line)
    {
        Stats result = null;
        String
        nameToken, // The current version token
        readsToken, // The current reads per second token
        writesToken, // The current writes per second token
        peakReadsToken, // The current peak reads token
        peakWritesToken // The current peak writes token
        ;
        double reads, writes, peakReads, peakWrites; // The double values of
                                                     // the corresponding
                                                     // tokens
            StringTokenizer tokenizer = new StringTokenizer( line, "\t" );
            if ( tokenizer.countTokens() < 5 )
            {
                return null;
            }
            // Grab the tokens
            nameToken = tokenizer.nextToken();
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
                return null;
            }
            result = new Stats(nameToken);
            result.avgReadsPerSec = reads;
            result.avgWritePerSec = writes;
            result.peakReadsPerSec = peakReads;
            result.peakWritesPerSec = peakWrites;
            return result;
    }

}
