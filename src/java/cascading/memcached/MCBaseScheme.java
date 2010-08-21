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

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
public abstract class MCBaseScheme<I, V> extends Scheme
  {

  public MCBaseScheme( Fields sinkFields )
    {
    setSinkFields( sinkFields );
    }

  @Override
  public boolean isSource()
    {
    return false;
    }

  @Override
  public void sourceInit( Tap tap, JobConf conf ) throws IOException
    {
    }

  @Override
  public void sinkInit( Tap tap, JobConf conf ) throws IOException
    {
    }

  @Override
  public Tuple source( Object key, Object value )
    {
    throw new IllegalStateException( "source should never be called" );
    }

  protected abstract I getIntermediate( TupleEntry tupleEntry );

  protected abstract String getKey( I intermediate );

  protected abstract V getValue( I intermediate );

  @Override
  public void sink( TupleEntry tupleEntry, OutputCollector outputCollector ) throws IOException
    {
    I intermediate = getIntermediate( tupleEntry );
    String key = getKey( intermediate );
    V value = getValue( intermediate );

    outputCollector.collect( key, value );
    }
  }
