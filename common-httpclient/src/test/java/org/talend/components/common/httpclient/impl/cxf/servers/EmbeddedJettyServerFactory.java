/*
 * Copyright (C) 2006-2023 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.common.httpclient.impl.cxf.servers;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.authentication.DigestAuthenticator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.security.Constraint;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;

@Slf4j
public class EmbeddedJettyServerFactory
        implements AbstractHTTPServerFactory<EmbeddedJettyServerFactory.JettyTestHTTPServer> {

    public final static String HTTP_OK = "/ok";

    private static EmbeddedJettyServerFactory instance;

    private Server server;

    private EmbeddedJettyServerFactory() {
    }

    public static synchronized EmbeddedJettyServerFactory getInstance() {
        if (instance == null) {
            instance = new EmbeddedJettyServerFactory();
        }

        return instance;
    }

    public EmbeddedJettyServerFactory.JettyTestHTTPServer createServer() {
        server = new Server(0);
        configureServer();

        try {
            server.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        log.debug(String.format("JettyTestHTTPServer started on port '%s'", port));

        return new JettyTestHTTPServer(server, port);

    }

    private void configureServer() {
        String realmResourceName = "jetty/realm.properties";
        ClassLoader classLoader = EmbeddedJettyServerFactory.class.getClassLoader();
        URL realmProps = classLoader.getResource(realmResourceName);
        if (realmProps == null) {
            String msg = String.format("Can't start the test HTTP server from %s : Unable to find %s",
                    EmbeddedJettyServerFactory.class.getName(), realmResourceName);
            log.error(msg);
            throw new RuntimeException(msg);
        }

        LoginService loginService = new HashLoginService("MyRealm", realmProps.toExternalForm());
        server.addBean(loginService);

        ConstraintSecurityHandler security = new ConstraintSecurityHandler();
        server.setHandler(security);

        Constraint constraint = new Constraint();
        constraint.setName("auth");
        constraint.setAuthenticate(true);
        constraint.setRoles(new String[] { "user", "admin" });

        ConstraintMapping mapping = new ConstraintMapping();
        mapping.setPathSpec("/*");
        mapping.setConstraint(constraint);

        security.setConstraintMappings(Collections.singletonList(mapping));
        security.setAuthenticator(new DigestAuthenticator());
        security.setLoginService(loginService);

        ServletHandler sh = new ServletHandler();
        sh.addServletWithMapping(EmbeddedJettyServerFactory.OKServlet.class, HTTP_OK);
        security.setHandler(sh);
    }

    public final static class OKServlet extends HttpServlet {

        protected void doGet(HttpServletRequest request, HttpServletResponse response)
                throws ServletException, IOException {
            response.setContentType("application/json");
            response.setStatus(HttpServletResponse.SC_OK);
            response.getWriter().println("{ \"status\": \"ok\"}");
            response.flushBuffer();
        }
    }

    @AllArgsConstructor
    public static class JettyTestHTTPServer implements AbstractHTTPServerFactory.TestHTTPServer<Server> {

        private Server server;

        private int port;

        @Override
        public void start() {
            log.debug("Jetty server should be already started to retrieve used port. Started: "
                    + this.server.isStarted());
            if (!this.server.isStarted()) {
                try {
                    this.server.start();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        }

        @Override
        public void stop() {
            try {
                this.server.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            log.debug("JettyTestHTTPServer stopped.");
        }

        @Override
        public Server getHttpServer() {
            return this.server;
        }

        @Override
        public int getPort() {
            return this.port;
        }
    }
}
