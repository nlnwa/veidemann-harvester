/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.db;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.commons.db.ChangeFeed;

/**
 *
 */
public abstract class ChangeFeedBase<T> implements ChangeFeed<T> {
    final Cursor cursor;

    public ChangeFeedBase(Cursor cursor) {
        this.cursor = cursor;
    }

    protected abstract Function<Map<String, Object>, T> mapper();

    @Override
    public Stream<T> stream() {
        return ((Stream) StreamSupport.stream(cursor.spliterator(), false).onClose(cursor::close)).map(mapper());
    }

    @Override
    public void close() {
        cursor.close();
    }

}
