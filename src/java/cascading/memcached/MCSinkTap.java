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

import cascading.tap.SinkTap;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 */
public class MCSinkTap extends SinkTap
  {
  String hostnames = null;
  boolean useBinaryProtocol = true;

  public MCSinkTap( String hostnames, MCBaseScheme scheme )
    {
    super( scheme );
    this.hostnames = hostnames;
    }

  public MCSinkTap( String hostnames, MCBaseScheme scheme, boolean useBinaryProtocol )
    {
    super( scheme );
    this.hostnames = hostnames;
    this.useBinaryProtocol = useBinaryProtocol;
    }

  public Path getPath()
    {
    return new Path( "memcached:/" + hostnames.replaceAll( ",|:", "_" ) );
    }

  @Override
  public boolean isWriteDirect()
    {
    return true;
    }

  public TupleEntryCollector openForWrite( JobConf conf ) throws IOException
    {
    return new MCOutputCollector( hostnames, useBinaryProtocol );
    }

  @Override
  public boolean makeDirs( JobConf conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean deletePath( JobConf conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean pathExists( JobConf conf ) throws IOException
    {
    return true;
    }

  @Override
  public long getPathModified( JobConf conf ) throws IOException
    {
    return System.currentTimeMillis();
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;
    if( !super.equals( object ) )
      return false;

    MCSinkTap mcSinkTap = (MCSinkTap) object;

    if( useBinaryProtocol != mcSinkTap.useBinaryProtocol )
      return false;
    if( hostnames != null ? !hostnames.equals( mcSinkTap.hostnames ) : mcSinkTap.hostnames != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( hostnames != null ? hostnames.hashCode() : 0 );
    result = 31 * result + ( useBinaryProtocol ? 1 : 0 );
    return result;
    }
  }
