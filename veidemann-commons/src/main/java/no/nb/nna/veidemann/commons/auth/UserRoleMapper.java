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

import no.nb.nna.veidemann.api.ConfigProto.Role;
import no.nb.nna.veidemann.api.ConfigProto.RoleMapping;
import no.nb.nna.veidemann.api.ControllerProto.RoleMappingsListRequest;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class UserRoleMapper {
    private static final Logger LOG = LoggerFactory.getLogger(UserRoleMapper.class);

    private static final ScheduledExecutorService updaterService = Executors.newSingleThreadScheduledExecutor();

    Map<String, Set<Role>> rolesByEmail = new HashMap<>();
    Map<String, Set<Role>> rolesByGroup = new HashMap<>();

    private final DbAdapter db;

    Lock roleUpdateLock = new ReentrantLock();

    public UserRoleMapper(DbAdapter db) {
        this.db = db;

        updaterService.scheduleAtFixedRate(() -> {
            updateRoleMappings();
        }, 0, 60, TimeUnit.SECONDS);
    }

    public Collection<Role> getRolesForUser(String email, Collection<String> groups) {
        roleUpdateLock.lock();
        try {
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
            LOG.debug("Get roles for user. Email: {}, Groups: {}, reolved to roles: {}", email, groups, roles);
            return roles;
        } finally {
            roleUpdateLock.unlock();
        }
    }

    private void updateRoleMappings() {
        LOG.trace("Update role mappings");
        Map<String, Set<Role>> rolesByEmailTmp = new HashMap<>();
        Map<String, Set<Role>> rolesByGroupTmp = new HashMap<>();

        db.listRoleMappings(RoleMappingsListRequest.getDefaultInstance())
                .getValueList().forEach(rm -> addRoleMapping(rm, rolesByEmailTmp, rolesByGroupTmp));

        roleUpdateLock.lock();
        try {
            rolesByEmail = rolesByEmailTmp;
            rolesByGroup = rolesByGroupTmp;
        } finally {
            roleUpdateLock.unlock();
        }
    }

    private void addRoleMapping(RoleMapping rm, Map<String, Set<Role>> emailRoles, Map<String, Set<Role>> groupRoles) {
        switch (rm.getEmailOrGroupCase()) {
            case EMAIL:
                LOG.trace("Adding role for email: {}, roles: {}", rm.getEmail(), rm.getRoleList());
                rm.getRoleList().forEach(role -> addRoleToList(emailRoles, rm.getEmail(), role));
                break;
            case GROUP:
                LOG.trace("Adding role for group: {}, roles: {}", rm.getGroup(), rm.getRoleList());
                rm.getRoleList().forEach(role -> addRoleToList(groupRoles, rm.getGroup(), role));
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
