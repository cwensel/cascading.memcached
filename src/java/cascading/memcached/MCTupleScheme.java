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
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class MCTupleEntryScheme will create a key from the keyFields values and try to store a Tuple from the
 * valueFields values.
 */
public class MCTupleScheme extends MCTupleEntryScheme<Tuple>
  {
  public MCTupleScheme( Fields keyFields, Fields valueFields )
    {
    this( keyFields, valueFields, ":" );
    }

  public MCTupleScheme( Fields keyFields, Fields valueFields, String keyDelim )
    {
    super( keyFields, valueFields, keyDelim );
    }

  protected Tuple getValue( TupleEntry tupleEntry )
    {
    return tupleEntry.selectTuple( getValueFields() );
    }
  }
