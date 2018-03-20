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

import com.nimbusds.jwt.JWTClaimsSet;
import io.grpc.BindableService;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import no.nb.nna.veidemann.api.ConfigProto.Role;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class IdTokenAuAuServerInterceptor implements AuAuServerInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(IdTokenAuAuServerInterceptor.class);

    public static final Metadata.Key<String> BEARER_TOKEN_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    public static final Listener NOOP_LISTENER = new Listener() {
    };

    private final UserRoleMapper userRoleMapper;

    private final IdTokenValidator idTokenValidator;

    private Grants grants = new Grants();

    public IdTokenAuAuServerInterceptor(UserRoleMapper userRoleMapper, IdTokenValidator idTokenValidator) {
        this.userRoleMapper = userRoleMapper;
        this.idTokenValidator = idTokenValidator;
    }

    /**
     * Add authorization to all requests made to this service.
     *
     * @param bindableService to intercept
     * @return the serviceDef with a authorization interceptor
     */
    @Override
    public ServerServiceDefinition intercept(BindableService bindableService) {
        ServerServiceDefinition def = bindableService.bindService();
        String serviceName = def.getServiceDescriptor().getName();
        Arrays.stream(bindableService.getClass().getDeclaredMethods())
                .forEach(m -> grants.addGrants(
                        serviceName + "/" + m.getName().substring(0, 1).toUpperCase() + m.getName().substring(1),
                        m.getDeclaredAnnotation(AllowedRoles.class)));

        return ServerInterceptors.intercept(def, this);
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call,
            Metadata requestHeaders,
            ServerCallHandler<ReqT, RespT> next) {

        String method = call.getMethodDescriptor().getFullMethodName();
        LOG.debug("Method: {}", method);

        Collection<Role> roles = new HashSet<>();

        // All users should have the ANY role. Even if their not logged in.
        roles.add(Role.ANY);

        // Check if user is logged in
        JWTClaimsSet claims = validateBearerToken(requestHeaders);
        if (claims == null) {
            if (grants.isRequireAuthenticatedUser(method)) {
                call.close(Status.UNAUTHENTICATED, new Metadata());
                return NOOP_LISTENER;
            } else {
                String email = (String) claims.getClaim("email");
                Context contextWithEmailAndRoles = Context.current()
                        .withValues(EmailContextKey.getKey(), email, RolesContextKey.getKey(), roles);
                return Contexts.interceptCall(contextWithEmailAndRoles, call, requestHeaders, next);
            }
        }

        // User is logged in so add ANY_USER independently of specific role assignments
        roles.add(Role.ANY_USER);

        String email = (String) claims.getClaim("email");
        List<String> groups = (List<String>) claims.getClaim("groups");

        LOG.debug("E-mail: {}", email);
        LOG.debug("Groups: {}", groups);

        roles = userRoleMapper.getRolesForUser(email, groups, roles);
        LOG.debug("Roles: {}", roles);

        // Check if user has required role
        if (!grants.isAllowed(method, roles)) {
            call.close(Status.PERMISSION_DENIED, new Metadata());
            return NOOP_LISTENER;
        }

        Context contextWithEmailAndRoles = Context.current()
                .withValues(EmailContextKey.getKey(), email, RolesContextKey.getKey(), roles);

        return Contexts.interceptCall(contextWithEmailAndRoles, call, requestHeaders, next);
    }

    private JWTClaimsSet validateBearerToken(Metadata requestHeaders) {
        String bearerToken = requestHeaders.get(BEARER_TOKEN_KEY);
        LOG.trace("Bearer token: {}", bearerToken);

        if (bearerToken != null && !bearerToken.isEmpty()) {
            String[] parts = bearerToken.split("\\s+", 2);
            if (parts.length == 2 && "bearer".equalsIgnoreCase(parts[0])) {
                JWTClaimsSet claims = idTokenValidator.verifyIdToken(parts[1]);
                LOG.trace("Claims: {}", claims);
                return claims;
            }
        }
        return null;
    }
}
