package io.mngr.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "mngr")
public class MngrProperties {
    private Store store = new Store();
    private String dataDir = "./data";
    private Auth auth = new Auth();
    private String masterKey = "dev-master-key-change-me";
    private boolean allowAdminOps = false;
    private Dispatch dispatch = new Dispatch();
    private Publish publish = new Publish();
    private Ws ws = new Ws();

    @Data public static class Store { private String mode = "h2"; }
    @Data public static class Auth { private String token = ""; }
    @Data public static class Dispatch { private int queueCapacity = 8192; private int workerPerSub = 1; }
    @Data public static class Publish {
        private int bufferCapacity = 10000;
        private String overflow = "DROP_OLDEST";
        private int maxRetries = 3;
    }
    @Data public static class Ws { private int sendQueueLimit = 2000; }

    public boolean isH2() { return "h2".equalsIgnoreCase(store.getMode()); }
}
