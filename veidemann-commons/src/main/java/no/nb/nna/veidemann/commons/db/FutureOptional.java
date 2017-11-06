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

package no.nb.nna.veidemann.commons.db;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 *
 */
public class FutureOptional<T> {
    private static final FutureOptional<?> EMPTY = new FutureOptional<>();

    /**
     * If non-null, the value; if null, indicates no value is present
     */
    private final T value;

    private final OffsetDateTime nextPossibleTime;

    /**
     * Constructs an empty instance.
     */
    private FutureOptional() {
        this.value = null;
        this.nextPossibleTime = null;
    }

    /**
     * Constructs an empty instance with a time for wich a new request for this FutureOptional might contain a value.
     */
    private FutureOptional(OffsetDateTime nextPossibleTime) {
        this.value = null;
        this.nextPossibleTime = nextPossibleTime;
    }

    /**
     * Constructs an instance with the value present.
     *
     * @param value the non-null value to be present
     * @throws NullPointerException if value is null
     */
    private FutureOptional(T value) {
        this.value = Objects.requireNonNull(value);
        this.nextPossibleTime = null;
    }

    /**
     * Returns an empty {@code FutureOptional} instance.  No value is present for this
     * FutureOptional.
     *
     * @param <T> Type of the non-existent value
     * @return an empty {@code Optional}
     */
    public static<T> FutureOptional<T> empty() {
        @SuppressWarnings("unchecked")
        FutureOptional<T> t = (FutureOptional<T>) EMPTY;
        return t;
    }

    public static<T> FutureOptional<T> emptyUntil(OffsetDateTime nextPossibleTime) {
        return new FutureOptional<>(nextPossibleTime);
    }

    /**
     * Returns an {@code FutureOptional} with the specified present non-null value.
     *
     * @param <T> the class of the value
     * @param value the value to be present, which must be non-null
     * @return an {@code Optional} with the value present
     * @throws NullPointerException if value is null
     */
    public static <T> FutureOptional<T> of(T value) {
        return new FutureOptional<>(value);
    }

    /**
     * If a value is present in this {@code FutureOptional}, returns the value,
     * otherwise throws {@code NoSuchElementException}.
     *
     * @return the non-null value held by this {@code FutureOptional}
     * @throws NoSuchElementException if there is no value present
     *
     * @see Optional#isPresent()
     */
    public T get() {
        if (value == null) {
            throw new NoSuchElementException("No value present");
        }
        return value;
    }

    public OffsetDateTime getWhen() {
        if (nextPossibleTime == null) {
            throw new NoSuchElementException("No future time present");
        }
        return nextPossibleTime;
    }

    public long getDelayMs() {
        if (nextPossibleTime == null) {
            throw new NoSuchElementException("No future time present");
        }
        long delay = OffsetDateTime.now().until(nextPossibleTime, ChronoUnit.MILLIS);
        if (delay < 0L) {
            delay = 0;
        }
        return delay;
    }

    /**
     * Return {@code true} if there is a value present, otherwise {@code false}.
     *
     * @return {@code true} if there is a value present, otherwise {@code false}
     */
    public boolean isPresent() {
        return value != null;
    }

    public boolean isMaybeInFuture() {
        return nextPossibleTime != null;
    }

    public boolean isEmpty() {
        return value == null && nextPossibleTime == null;
    }

    /**
     * If a value is present, invoke the specified consumer with the value,
     * otherwise do nothing.
     *
     * @param consumer block to be executed if a value is present
     * @throws NullPointerException if value is present and {@code consumer} is
     * null
     */
    public void ifPresent(Consumer<? super T> consumer) {
        if (value != null)
            consumer.accept(value);
    }

    public void ifMaybeInFuture(Consumer<OffsetDateTime> consumer) {
        if (nextPossibleTime != null)
            consumer.accept(nextPossibleTime);
    }

    @Override
    public String toString() {
        if (value != null) {
            return String.format("Optional[%s]", value);
        }
        if (nextPossibleTime != null) {
            return String.format("Optional.futureTime[%s]", nextPossibleTime);
        }
        return "Optional.empty";
    }
}
