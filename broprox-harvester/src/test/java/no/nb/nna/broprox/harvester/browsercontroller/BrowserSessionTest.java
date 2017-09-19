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
package no.nb.nna.broprox.harvester.browsercontroller;

import java.util.List;

import io.grpc.protobuf.ProtoUtils;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.model.ConfigProto;
import no.nb.nna.broprox.model.MessagesProto;
import org.junit.Test;
//import static org.junit.Assert.*;

import static org.assertj.core.api.Assertions.*;

/**
 *
 */
public class BrowserSessionTest {

    public BrowserSessionTest() {
    }

    /**
     * Test of getExecutionId method, of class BrowserSession.
     */
    @Test
    public void testGetExecutionId() {
        String v1 = "";
        Double v2 = 0d;
        Integer v3 = null;
        System.out.println(">> '" + ensureNotNull(v1) + "'");
        MessagesProto.Cookie c = MessagesProto.Cookie.newBuilder()
                .setName(v1)
                .setExpires(v2)
                .setSize(v3 == null ? 0 : v3)
                .build();
        System.out.println("c: " + c);
        fail("");
    }

    public String ensureNotNull(String val) {

        if (val == null) {
            return "";
        }
        return val;
    }

}
