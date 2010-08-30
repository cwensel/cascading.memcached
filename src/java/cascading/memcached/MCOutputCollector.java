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
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import cascading.tap.TapException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MCOutputCollector<V> extends TupleEntryCollector implements OutputCollector<String, V>
  {
  private static final Logger LOG = LoggerFactory.getLogger( MCOutputCollector.class );

  private MemcachedClient client;
  private int shutdownTimeoutSec = 5;
  private int flushThreshold = 1000;

  private LinkedList<Future> futures = new LinkedList<Future>();

  MCOutputCollector( String hostnames ) throws IOException
    {
    this( hostnames, true );
    }

  MCOutputCollector( String hostnames, boolean useBinary ) throws IOException
    {
    this( hostnames, useBinary, 1 );
    }

  MCOutputCollector( String hostnames, boolean useBinary, int shutdownTimeoutSec ) throws IOException
    {
    this( hostnames, useBinary, shutdownTimeoutSec, 1000 );
    }

  MCOutputCollector( String hostnames, boolean useBinary, int shutdownTimeoutSec, int flushThreshold ) throws IOException
    {
    this.shutdownTimeoutSec = shutdownTimeoutSec;
    this.flushThreshold = flushThreshold;
    ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();

    ConnectionFactoryBuilder.Protocol protocol = useBinary ? ConnectionFactoryBuilder.Protocol.BINARY : ConnectionFactoryBuilder.Protocol.TEXT;

    builder = builder.setProtocol( protocol ).setOpQueueMaxBlockTime( 1000 );

    client = new MemcachedClient( builder.build(), AddrUtil.getAddresses( hostnames ) );
    }

  @Override
  public void collect( String key, V value ) throws IOException
    {
    Future<Boolean> future = retry( key, value, 2000, 3 );

    if( future == null )
      throw new TapException( "unable to store value" );

    futures.add( future );

    if( futures.size() >= flushThreshold )
      fullFlush();
    }

  private Future<Boolean> retry( String key, Object value, int duration, int tries )
    {
    if( tries == 0 )
      return null;

    try
      {
      return client.set( key, 0, value );
      }
    catch( IllegalStateException exception )
      {
      LOG.warn( "retrying set operation" );
      sleepSafe( duration );
      return retry( key, value, duration * 2, tries - 1 );
      }
    }

  private void sleepSafe( int duration )
    {
    try
      {
      Thread.sleep( duration );
      }
    catch( InterruptedException exception )
      {
      // do nothing
      }
    }

  private void fullFlush()
    {
    while( !futures.isEmpty() )
      flush();
    }

  private void flush()
    {
    ListIterator<Future> iterator = futures.listIterator();

    while( iterator.hasNext() )
      {
      Future future = iterator.next();

      if( future.isCancelled() )
        throw new TapException( "operation was canceled" );

      if( future.isDone() )
        iterator.remove();
      }
    }

  @Override
  protected void collect( Tuple tuple )
    {
    }

  @Override
  public void close()
    {
    try
      {
      fullFlush();
      }
    finally
      {
      client.shutdown( shutdownTimeoutSec, TimeUnit.SECONDS );
      }
    }
  }
