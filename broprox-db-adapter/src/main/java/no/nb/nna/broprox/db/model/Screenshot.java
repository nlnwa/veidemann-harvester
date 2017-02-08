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

import no.nb.nna.broprox.db.DbObject;

/**
 *
 */
public interface Screenshot extends DbObject<Screenshot> {

    Screenshot withId(String id);

    String getId();

    Screenshot withExecutionId(String id);

    String getExecutionId();

    Screenshot withUri(String uri);

    String getUri();

    Screenshot withImg(byte[] img);

    byte[] getImg();

}
