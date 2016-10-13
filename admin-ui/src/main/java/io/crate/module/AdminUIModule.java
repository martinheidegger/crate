/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.module;

import com.google.common.util.concurrent.SettableFuture;
import io.crate.Version;
import io.crate.plugin.CrateCorePlugin;
import io.crate.rest.CrateRestFilter;
import io.crate.rest.CrateRestMainAction;
import io.crate.rest.action.admin.AdminUIFrontpageAction;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.matcher.AbstractMatcher;
import org.elasticsearch.common.inject.spi.InjectionListener;
import org.elasticsearch.common.inject.spi.TypeEncounter;
import org.elasticsearch.common.inject.spi.TypeListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.action.admin.cluster.node.info.RestNodesInfoAction;
import org.elasticsearch.rest.action.main.RestMainAction;

import java.util.concurrent.TimeUnit;

public class AdminUIModule extends AbstractModule {

    private final ESLogger logger;

    public AdminUIModule(Settings settings) {
        logger = Loggers.getLogger(getClass().getPackage().getName(), settings);
    }

    @Override
    protected void configure() {
        Version version = Version.CURRENT;
        logger.info("configuring crate. version: {}", version);


        /*
         * This is a rather hacky method to overwrite the handler for "/"
         * The ES plugins are loaded before the core ES components. That means that the registration for
         * "/" in {@link AdminUIFrontpageAction} is overwritten once {@link RestMainAction} is instantiated.
         *
         * By using a listener that is called after {@link RestMainAction} is instantiated we can call
         * {@link io.crate.rest.AdminUIFrontpageAction#registerHandler()}  and overwrite it "back".
         */

        // the adminUIListener is used to retrieve the AdminUIFrontpageAction instance.
        // otherwise there is no way to retrieve it at this point.
        AdminUIFrontpageActionListener adminUIListener = new AdminUIFrontpageActionListener();
        bindListener(
            new SubclassOfMatcher(AdminUIFrontpageAction.class),
            adminUIListener);

        // this listener will use the AdminUIFrontpageAction instance and call registerHandler
        // after RestMainAction is created.
        bindListener(
            new SubclassOfMatcher(RestMainAction.class),
            new RestMainActionListener(adminUIListener.instanceFuture));


    }

    private class RestMainActionListener implements TypeListener {

        private final SettableFuture<AdminUIFrontpageAction> instanceFuture;

        RestMainActionListener(SettableFuture<AdminUIFrontpageAction> instanceFuture) {
            this.instanceFuture = instanceFuture;
        }

        @Override
        public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
            encounter.register(new InjectionListener<I>() {
                @Override
                public void afterInjection(I injectee) {
                    try {
                        AdminUIFrontpageAction adminUIFrontpageAction = instanceFuture.get(10, TimeUnit.SECONDS);
                        adminUIFrontpageAction.registerHandler();
                    } catch (Exception e) {
                        logger.error("Could not register adminUIFrontpageAction handler", e);
                    }
                }
            });
        }
    }

    private static class SubclassOfMatcher extends AbstractMatcher<TypeLiteral<?>> {

        private final Class<?> klass;

        SubclassOfMatcher(Class<?> klass) {
            this.klass = klass;
        }

        @Override
        public boolean matches(TypeLiteral<?> typeLiteral) {
            return klass.isAssignableFrom(typeLiteral.getRawType());
        }
    }

    private static class AdminUIFrontpageActionListener implements TypeListener {

        private final SettableFuture<AdminUIFrontpageAction> instanceFuture;

        AdminUIFrontpageActionListener() {
            this.instanceFuture = SettableFuture.create();

        }

        @Override
        public <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter) {
            encounter.register(new InjectionListener<I>() {
                @Override
                public void afterInjection(I injectee) {
                    instanceFuture.set((AdminUIFrontpageAction) injectee);
                }
            });
        }
    }
}
