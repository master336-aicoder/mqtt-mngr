package io.mngr.config;

import io.mngr.store.CredCipher;
import io.mngr.store.file.FileConfigRepository;
import io.mngr.store.file.FileRuntimeRepository;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class StoreConfig {

    @Bean
    public CredCipher credCipher(MngrProperties props) {
        return new CredCipher(props);
    }

    @Bean
    @ConditionalOnProperty(name = "mngr.store.mode", havingValue = "file")
    public io.mngr.core.spi.ConfigRepository fileConfigRepository(MngrProperties props, CredCipher cipher) {
        return new FileConfigRepository(props, cipher);
    }

    @Bean
    @ConditionalOnProperty(name = "mngr.store.mode", havingValue = "file")
    public io.mngr.core.spi.RuntimeRepository fileRuntimeRepository(MngrProperties props) {
        return new FileRuntimeRepository(props);
    }
}
