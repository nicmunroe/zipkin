/**
 * Copyright 2015-2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.server;

import com.nike.riposte.server.Server;
import com.nike.riposte.server.config.ServerConfig;
import com.nike.riposte.server.http.Endpoint;
import com.nike.riposte.server.http.filter.RequestAndResponseFilter;
import com.nike.riposte.server.http.filter.impl.AllowAllTheThingsCORSFilter;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import zipkin.server.brave.BootstrapTrace;

import static java.util.Collections.singletonList;

@SpringBootApplication
@EnableZipkinServer
public class ZipkinServer {

  public static void main(String[] args) throws CertificateException, InterruptedException, IOException {
    ConfigurableApplicationContext cac = new SpringApplicationBuilder(ZipkinServer.class)
        .listeners(new RegisterZipkinHealthIndicators(), BootstrapTrace.INSTANCE::record)
        .properties("spring.config.name=zipkin-server").run(args);

//    AnnotationConfigApplicationContext rootContext = new AnnotationConfigApplicationContext(ZipkinServer.class);
//    rootContext.addApplicationListener(new RegisterZipkinHealthIndicators());
//    rootContext.addApplicationListener(BootstrapTrace.INSTANCE::record);

    Server server = new Server(new RiposteServerConfig(cac));
    server.startup();
  }

  public static class RiposteServerConfig implements ServerConfig {
    private final Collection<Endpoint<?>> endpoints;
    private final List<RequestAndResponseFilter> requestAndResponseFilters =
      singletonList(new AllowAllTheThingsCORSFilter());

    public RiposteServerConfig(ApplicationContext appContext) {
      endpoints = Arrays.asList(
        appContext.getBean(ZipkinHttpCollector.RiposteSpanIngestEndpoint.class)
      );
    }

    @Override
    public Collection<Endpoint<?>> appEndpoints() {
      return endpoints;
    }

    @Override
    public int endpointsPort() {
      return 8042;
    }

    @Override
    public List<RequestAndResponseFilter> requestAndResponseFilters() {
      return requestAndResponseFilters;
    }
  }

}
