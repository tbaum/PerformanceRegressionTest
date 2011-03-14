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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.DateFormat;
import java.util.Date;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.Address;
import javax.mail.Authenticator;
import javax.mail.BodyPart;
import javax.mail.Message.RecipientType;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

public class SampleMail
{
    public static void mail( String attachement ) throws Exception
    {
        InputStream in = null;
        final Properties props = new Properties();
        try
        {
            in = new FileInputStream( "mail.properties" );
            props.load( in );
        }
        finally
        {
            if ( in != null )
            {
                in.close();
            }
        }
        Authenticator auth = new Authenticator()
        {
            @Override
            protected PasswordAuthentication getPasswordAuthentication()
            {
                return new PasswordAuthentication(
                        props.getProperty( "mail.username" ),
                        props.getProperty( "mail.password" ) );
            }
        };
        Session session = Session.getDefaultInstance( props, auth );

        MimeMessage message = new MimeMessage( session );
        message.setSubject( "Performance regression test failed: "
                            + DateFormat.getInstance().format( new Date() ) );
        Address address = new InternetAddress( "perftest@neotechnology.com",
                "Performance Tester" );
        message.setFrom( address );

        for ( Address add : MailToList.getAddresses() )
        {
            message.addRecipient( RecipientType.BCC, add );
        }

        // Create the message part
        BodyPart messageBodyPart = new MimeBodyPart();

        // Fill the message
        messageBodyPart.setText( "Test Failed" );

        Multipart multipart = new MimeMultipart();
        multipart.addBodyPart( messageBodyPart );

        // Part two is attachment
        File f = new File( attachement );
        // for ( String filename : f.list( new FilenameFilter()
        // {
        //
        // @Override
        // public boolean accept( File dir, String name )
        // {
        // return name.endsWith( "gz" );
        // }
        //
        // } ) )
        DataSource source = new FileDataSource( f );
        messageBodyPart = new MimeBodyPart();
        messageBodyPart.setDataHandler( new DataHandler( source ) );
        messageBodyPart.setFileName( f.getName() );
        multipart.addBodyPart( messageBodyPart );

        // Put parts in message
        message.setContent( multipart );

        Transport.send( message );
    }
}
