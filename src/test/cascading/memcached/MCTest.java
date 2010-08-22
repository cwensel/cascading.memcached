/*
 * Copyright (c) 2010 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.memcached;

import java.io.IOException;

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.TextDelimited;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;

/**
 *
 */
public class MCTest extends ClusterTestCase
  {
  String inputFile = "src/test/data/small.txt";

  public MCTest()
    {
    super( "memcached tests" );

    }

  private MemcachedClient getClient() throws IOException
    {
    ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
    builder = builder.setProtocol( ConnectionFactoryBuilder.Protocol.BINARY );
    builder = builder.setOpQueueMaxBlockTime( 1000 );

    return new MemcachedClient( builder.build(), AddrUtil.getAddresses( "localhost:11211" ) );
    }

//    1 c
//    2 d
//    3 c
//    4 d
//    5 e

  public void testTupleScheme() throws IOException
    {
    runTupleTest( true );
    runTupleTest( false );
    }

  private void runTupleTest( boolean useBinary ) throws IOException
    {
    runTestFor( (MCBaseScheme) new MCTupleScheme( new Fields( "num" ), new Fields( "lower" ) ), useBinary );

    MemcachedClient client = getClient();

    assertEquals( "c", ( (Tuple) client.get( "1" ) ).get( 0 ) );
    assertEquals( "d", ( (Tuple) client.get( "2" ) ).get( 0 ) );
    assertEquals( "c", ( (Tuple) client.get( "3" ) ).get( 0 ) );
    assertEquals( "d", ( (Tuple) client.get( "4" ) ).get( 0 ) );
    assertEquals( "e", ( (Tuple) client.get( "5" ) ).get( 0 ) );

    client.shutdown();
    }

  public void testDelimitedScheme() throws IOException
    {
    runDelimitedTest( true );
    runDelimitedTest( false );
    }

  private void runDelimitedTest( boolean useBinary ) throws IOException
    {
    runTestFor( (MCBaseScheme) new MCDelimitedScheme( new Fields( "num" ), new Fields( "lower" ) ), useBinary );

    MemcachedClient client = getClient();

    assertEquals( "c", client.get( "1" ) );
    assertEquals( "d", client.get( "2" ) );
    assertEquals( "c", client.get( "3" ) );
    assertEquals( "d", client.get( "4" ) );
    assertEquals( "e", client.get( "5" ) );

    client.shutdown();
    }

  private void runTestFor( MCBaseScheme scheme, boolean useBinary )
    {
    Tap source = new Hfs( new TextDelimited( new Fields( "num", "lower", "upper" ), " " ), inputFile );

    Tap sink = new MCSinkTap( "localhost:11211", scheme, useBinary );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, new Pipe( "identity" ) );

    flow.complete();
    }

  }
