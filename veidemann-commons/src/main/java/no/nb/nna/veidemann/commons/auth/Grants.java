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
package no.nb.nna.veidemann.commons.auth;

import no.nb.nna.veidemann.api.config.v1.Role;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class Grants {
    Map<String, Grant> grantsForMethod = new HashMap<>();

    protected void addGrants(String method, AllowedRoles allowedRolesAnnotation) {
        if (allowedRolesAnnotation != null && !Arrays.asList(allowedRolesAnnotation.value()).contains(Role.ANY)) {
            grantsForMethod.put(method, new Grant(allowedRolesAnnotation.value()));
        }
    }

    public boolean isRequireAuthenticatedUser(String method) {
        return grantsForMethod.get(method) != null;
    }

    public boolean isAllowed(String method, Collection<Role> roles) {
        Grant grant = grantsForMethod.get(method);
        if (grant == null) {
            return true;
        }
        return grant.isAllowed(roles);
    }

    class Grant {
        private final Role[] allowedRoles;

        public Grant(Role[] allowedRoles) {
            this.allowedRoles = allowedRoles;
        }

        boolean isAllowed(Collection<Role> roles) {
            for (Role allowedRole : allowedRoles) {
                if (allowedRole == Role.ANY_USER) {
                    return true;
                }
                for (Role role : roles) {
                    if (allowedRole == role) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
