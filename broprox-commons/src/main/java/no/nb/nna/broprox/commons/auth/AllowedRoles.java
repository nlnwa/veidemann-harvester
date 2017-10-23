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
package no.nb.nna.broprox.commons.auth;

import no.nb.nna.broprox.model.ConfigProto.Role;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Set the roles which should be allowed to execute the annotated method.
 * <p>
 * A method without AllowedRoles annotation is the same as setting role {@link Role#ANY} which doesn't require
 * athentication to get access. A method with an empty AllowedRoles annotation is the same as setting
 * {@link Role#ANY_USER} which allows access to all authenticated users.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AllowedRoles {
    Role[] value() default {Role.ANY_USER};
}
