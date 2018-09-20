/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.db;

import com.rethinkdb.RethinkDB;
import org.assertj.core.data.Percentage;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class RethinkDbCrawlQueueAdapterTest {
    static final RethinkDB r = RethinkDB.r;

    @Test
    public void getWeightedRandomExecutionId() {
        Percentage allowedDifference = Percentage.withPercentage(10);
        List<Map<String, Object>> executionIds = r.array(
                r.hashMap("executionId", "eid1").with("priorityWeight", 1d),
                r.hashMap("executionId", "eid2").with("priorityWeight", 1d)
        );

        Map<String, Integer> resultCounts = run10000Iterations(executionIds);
        assertThat(resultCounts.get("eid1")).isCloseTo(5000, allowedDifference);
        assertThat(resultCounts.get("eid2")).isCloseTo(5000, allowedDifference);

        executionIds = r.array(
                r.hashMap("executionId", "eid1").with("priorityWeight", 1d),
                r.hashMap("executionId", "eid2").with("priorityWeight", 2d)
        );

        resultCounts = run10000Iterations(executionIds);
        assertThat(resultCounts.get("eid1")).isCloseTo(3333, allowedDifference);
        assertThat(resultCounts.get("eid2")).isCloseTo(6666, allowedDifference);

        executionIds = r.array(
                r.hashMap("executionId", "eid1").with("priorityWeight", 1d),
                r.hashMap("executionId", "eid2").with("priorityWeight", 1d),
                r.hashMap("executionId", "eid3").with("priorityWeight", 2d)
        );

        resultCounts = run10000Iterations(executionIds);
        assertThat(resultCounts.get("eid1")).isCloseTo(2500, allowedDifference);
        assertThat(resultCounts.get("eid2")).isCloseTo(2500, allowedDifference);
        assertThat(resultCounts.get("eid3")).isCloseTo(5000, allowedDifference);
    }

    private Map<String, Integer> run10000Iterations(List<Map<String, Object>> executionIds) {
        Map<String, Integer> resultCounts = r.hashMap();
        for (int i = 0; i < 10000; i++) {
            String result = RethinkDbCrawlQueueAdapter.getWeightedRandomExecutionId(executionIds);

            resultCounts.put(result, resultCounts.get(result) == null ? 1 : resultCounts.get(result) + 1);
        }
        return resultCounts;
    }
}