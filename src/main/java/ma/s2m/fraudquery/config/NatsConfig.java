package ma.s2m.fraudquery.config;

import java.io.IOException;
import java.time.Duration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.nats.client.Connection;
import io.nats.client.Nats;
import ma.s2m.functions.Function;
import ma.s2m.repository.IRepository;
import ma.s2m.repository.PropertiesRepository;

// ===== NATS Configuration =====
@Configuration
class NatsConfig {

    IRepository centralRepository;
    String timeoutString;

    @Bean(destroyMethod = "close")
    public Connection natsConnection() throws Exception {

        // Create a singleton NATS connection for the app; Spring will close it on shutdown
        IRepository localPropertyFile = new PropertiesRepository("application.properties");
        String repositoryClassName = localPropertyFile.getProperty("app.processor.repository.class", "ma.s2m.repository.PropertiesRepository");
        centralRepository = (IRepository) Function.createDynamicClass(repositoryClassName);
        centralRepository.load();

        String natsHost = centralRepository.getProperty("nats.host", "localhost");
        int natsPort = Integer.parseInt(centralRepository.getProperty("nats.port", "4222"));
        return Nats.connect("nats://" + natsHost + ":" + natsPort);
    }

    @Bean
    public Duration fraudQueryTimeout() throws IOException {
        // Create a singleton NATS connection for the app; Spring will close it on shutdown
        IRepository localPropertyFile = new PropertiesRepository("application.properties");
        String repositoryClassName = localPropertyFile.getProperty("app.processor.repository.class", "ma.s2m.repository.PropertiesRepository");
        centralRepository = (IRepository) Function.createDynamicClass(repositoryClassName);
        centralRepository.load();
        timeoutString = centralRepository.getProperty("fraud.query.timeout", "5s");
        return Duration.parse("PT" + timeoutString.toUpperCase());
    }
}
