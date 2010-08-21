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

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *
 */
public abstract class MCFieldedScheme<V> extends MCBaseScheme<V>
  {
  private String keyDelim = ":";
  private Fields keyFields;
  private Fields valueFields;

  public MCFieldedScheme( Fields keyFields, Fields valueFields )
    {
    super( Fields.merge( keyFields, valueFields ) );
    this.keyFields = keyFields;
    this.valueFields = valueFields;
    }

  public String getKeyDelim()
    {
    return keyDelim;
    }

  public Fields getKeyFields()
    {
    return keyFields;
    }

  public Fields getValueFields()
    {
    return valueFields;
    }

  protected String getKey( TupleEntry tupleEntry )
    {
    return tupleEntry.selectTuple( getKeyFields() ).toString( getKeyDelim(), false );
    }

  protected abstract V getValue( TupleEntry tupleEntry );
  }
