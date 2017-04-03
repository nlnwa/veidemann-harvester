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
package no.nb.nna.broprox.db.model;

import java.time.OffsetDateTime;

import no.nb.nna.broprox.db.DbObject;

/**
 *
 */
public interface CrawlExecutionStatus extends DbObject<CrawlExecutionStatus> {

    enum State {

        CREATED,
        RUNNING,
        FINISHED,
        ABORTED_TIMEOUT,
        ABORTED_SIZE,
        ABORTED_MANUAL,
        FAILED,
        DIED

    }

    CrawlExecutionStatus withId(String id);

    String getId();

    CrawlExecutionStatus withState(State state);

    State getState();

    CrawlExecutionStatus withStartTime(OffsetDateTime timestamp);

    OffsetDateTime getStartTime();

    CrawlExecutionStatus withEndTime(OffsetDateTime timestamp);

    OffsetDateTime getEndTime();

    CrawlExecutionStatus withDocumentsCrawled(long count);

    long getDocumentsCrawled();

    CrawlExecutionStatus withBytesCrawled(long count);

    long getBytesCrawled();

}
