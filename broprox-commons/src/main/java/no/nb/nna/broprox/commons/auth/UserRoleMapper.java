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

import no.nb.nna.broprox.api.ControllerProto.RoleMappingsListRequest;
import no.nb.nna.broprox.commons.db.DbAdapter;
import no.nb.nna.broprox.model.ConfigProto.Role;
import no.nb.nna.broprox.model.ConfigProto.RoleMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UserRoleMapper {
    private static final Logger LOG = LoggerFactory.getLogger(UserRoleMapper.class);

    Map<String, Set<Role>> rolesByEmail = new HashMap<>();
    Map<String, Set<Role>> rolesByGroup = new HashMap<>();

    private final DbAdapter db;

    public UserRoleMapper(DbAdapter db) {
        this.db = db;
        updateRoleMappings();
    }

    private void updateRoleMappings() {
        db.listRoleMappings(RoleMappingsListRequest.getDefaultInstance())
                .getValueList().forEach(rm -> addRoleMapping(rm));
    }

    public Collection<Role> getRolesForUser(String email, Collection<String> groups) {
        Set<Role> roles = new HashSet<>();
        if (email != null && rolesByEmail.containsKey(email)) {
            roles.addAll(rolesByEmail.get(email));
        }
        if (groups != null) {
            for (String group : groups) {
                if (rolesByGroup.containsKey(group)) {
                    roles.addAll(rolesByGroup.get(group));
                }
            }
        }
        return roles;
    }

    private void addRoleMapping(RoleMapping rm) {
        switch (rm.getEmailOrGroupCase()) {
            case EMAIL:
                rm.getRoleList().forEach(role -> addRoleToList(rolesByEmail, rm.getEmail(), role));
                break;
            case GROUP:
                rm.getRoleList().forEach(role -> addRoleToList(rolesByGroup, rm.getEmail(), role));
                break;
        }
    }

    private void addRoleToList(Map<String, Set<Role>> roles, String key, Role role) {
        Set<Role> roleList = roles.get(key);
        if (roleList == null) {
            roleList = new HashSet<>();
            roles.put(key, roleList);
        }
        roleList.add(role);
    }
}
