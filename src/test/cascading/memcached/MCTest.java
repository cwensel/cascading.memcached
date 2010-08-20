/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

import cascading.ClusterTestCase;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.TextDelimited;
import cascading.tap.Hfs;
import cascading.tap.Tap;
import cascading.tuple.Fields;

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

  public void testMemcached()
    {
    Tap source = new Hfs( new TextDelimited( new Fields( "num", "lower", "upper" ), " " ), inputFile );

    Fields keyFields = new Fields( "num" );
    Fields valueFields = new Fields( "lower" );

    Tap sink = new MCSinkTap( "localhost:11211", keyFields, valueFields );

    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, new Pipe( "identity" ) );

    flow.complete();
    }

  }
