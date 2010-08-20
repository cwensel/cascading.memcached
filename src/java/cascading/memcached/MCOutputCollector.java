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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.util.CacheLoader;
import org.apache.hadoop.mapred.OutputCollector;

/**
 *
 */
public class MCOutputCollector extends TupleEntryCollector implements OutputCollector<String, Tuple>
  {
  private MemcachedClient client;
  private CacheLoader cacheLoader;

  public MCOutputCollector( String hostnames ) throws IOException
    {
    ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();

    ConnectionFactoryBuilder.Protocol protocol = ConnectionFactoryBuilder.Protocol.BINARY;

    builder = builder.setProtocol( protocol ).setOpQueueMaxBlockTime( 1000 );

    client = new MemcachedClient( builder.build(), AddrUtil.getAddresses( hostnames ) );
    cacheLoader = new CacheLoader( client );
    }

  @Override
  protected void collect( Tuple tuple )
    {
    }

  @Override
  public void close()
    {
    client.shutdown( 60, TimeUnit.SECONDS );
    }

  @Override
  public void collect( String key, Tuple value ) throws IOException
    {
    cacheLoader.push( key, value );
    }
  }
