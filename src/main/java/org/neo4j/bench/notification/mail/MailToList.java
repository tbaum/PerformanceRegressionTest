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

package org.neo4j.bench.notification.mail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.mail.Address;
import javax.mail.internet.InternetAddress;

class MailToList
{
    private final List<Address> addresses;

    private static final String FILENAME = "addresses.txt";

    private static MailToList _instance = null;

    MailToList() throws Exception
    {
        addresses = new LinkedList<Address>();
        fillAddresses( addresses );
    }

    static List<Address> getAddresses() throws Exception
    {
        if ( _instance == null )
        {
            _instance = new MailToList();
        }
        return Collections.unmodifiableList( _instance.addresses );
    }

    private static void fillAddresses( List<Address> toFill ) throws Exception
    {
        BufferedReader r = null;
        try
        {
            r = new BufferedReader( new FileReader( FILENAME ) );
            String address;
            while ( ( address = r.readLine() ) != null )
            {
                address = address.trim();
                if ( address.startsWith( "#" ) || address.length() < 3 )
                {
                    continue;
                }
                toFill.add( new InternetAddress( address ) );
            }
        }
        finally
        {
            if ( r != null )
            {
                r.close();
            }
        }
    }
}
