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
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.util.CacheLoader;
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
  private CacheLoader cacheLoader;
  private int replyTimeoutMin = 1;
  private int flushThreshold = 1000;

  private List<Future> futures = new LinkedList<Future>();

  MCOutputCollector( String hostnames ) throws IOException
    {
    this( hostnames, true );
    }

  MCOutputCollector( String hostnames, boolean useBinary ) throws IOException
    {
    this( hostnames, useBinary, 1 );
    }

  MCOutputCollector( String hostnames, boolean useBinary, int replyTimeoutMin ) throws IOException
    {
    this( hostnames, useBinary, replyTimeoutMin, 1000 );
    }

  MCOutputCollector( String hostnames, boolean useBinary, int replyTimeoutMin, int flushThreshold ) throws IOException
    {
    this.replyTimeoutMin = replyTimeoutMin;
    this.flushThreshold = flushThreshold;
    ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();

    ConnectionFactoryBuilder.Protocol protocol = useBinary ? ConnectionFactoryBuilder.Protocol.BINARY : ConnectionFactoryBuilder.Protocol.TEXT;

    builder = builder.setProtocol( protocol ).setOpQueueMaxBlockTime( 1000 );

    client = new MemcachedClient( builder.build(), AddrUtil.getAddresses( hostnames ) );
    cacheLoader = new CacheLoader( client );
    }

  @Override
  public void collect( String key, V value ) throws IOException
    {
    futures.add( cacheLoader.push( key, value ) );

    if( futures.size() >= flushThreshold )
      flush();
    }

  private void flush()
    {
    try
      {
      for( Future future : futures )
        {
        try
          {
          future.get( replyTimeoutMin, TimeUnit.MINUTES );
          }
        catch( Exception exception )
          {
          LOG.warn( "failed receiving value", exception );
          }
        }
      }
    finally
      {
      futures.clear();
      }
    }

  @Override
  protected void collect( Tuple tuple )
    {
    }

  @Override
  public void close()
    {
    flush();
    client.shutdown( replyTimeoutMin, TimeUnit.MINUTES );
    }
  }
