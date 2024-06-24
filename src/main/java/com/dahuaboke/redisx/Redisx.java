package com.dahuaboke.redisx;


import com.dahuaboke.redisx.utils.YamlUtil;

import java.net.InetSocketAddress;
import java.util.List;

public class Redisx {

    public static void main(String[] args) {
        Config config = YamlUtil.parseYamlParam();
        Controller controller = new Controller(config.fromIsCluster(), config.getFromPassword(), config.toIsCluster(), config.getToPassword(), config.isImmediate());
        controller.start(config.getFromAddresses(), config.getToAddresses(), config.consoleEnable(),
                config.getConsolePort(), config.getConsoleTimeout(), config.isAlwaysFullSync());
    }

    public static class Config {
        private From from;
        private To to;
        private Console console;
        private boolean immediate;
        private boolean alwaysFullSync;

        public Config(boolean fromIsCluster, String fromPassword, List<InetSocketAddress> fromAddresses, boolean toIsCluster, String toPassword,
                      List<InetSocketAddress> toAddresses, boolean consoleEnable, int consolePort, int consoleTimeout, boolean immediate, boolean alwaysFullSync) {
            this.from = new From(fromIsCluster, fromAddresses, fromPassword);
            this.to = new To(toIsCluster, toAddresses, toPassword);
            this.console = new Console(consoleEnable, consolePort, consoleTimeout);
            this.immediate = immediate;
            this.alwaysFullSync = alwaysFullSync;
        }

        public boolean fromIsCluster() {
            return this.from.isCluster;
        }

        public List<InetSocketAddress> getFromAddresses() {
            return this.from.addresses;
        }

        public boolean toIsCluster() {
            return this.to.isCluster;
        }

        public List<InetSocketAddress> getToAddresses() {
            return this.to.addresses;
        }

        public boolean consoleEnable() {
            return this.console.enable;
        }

        public int getConsolePort() {
            return this.console.port;
        }

        public int getConsoleTimeout() {
            return this.console.timeout;
        }

        public String getFromPassword() {
            return this.from.password;
        }

        public String getToPassword() {
            return this.to.password;
        }

        public boolean isImmediate() {
            return immediate;
        }

        public boolean isAlwaysFullSync() {
            return alwaysFullSync;
        }

        private static class From {
            private boolean isCluster;
            private List<InetSocketAddress> addresses;
            private String password;

            public From(boolean isCluster, List<InetSocketAddress> addresses, String password) {
                this.isCluster = isCluster;
                this.addresses = addresses;
                this.password = password;
            }
        }

        private static class To {
            private boolean isCluster;
            private List<InetSocketAddress> addresses;
            private String password;

            public To(boolean isCluster, List<InetSocketAddress> addresses, String password) {
                this.isCluster = isCluster;
                this.addresses = addresses;
                this.password = password;
            }
        }

        private static class Console {
            private boolean enable;
            private int port;
            private int timeout;

            public Console(boolean enable, int port, int timeout) {
                this.enable = enable;
                this.port = port;
                this.timeout = timeout;
            }
        }
    }
}