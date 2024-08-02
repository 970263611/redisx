package com.dahuaboke.redisx;


import com.dahuaboke.redisx.annotation.FieldOrm;
import com.dahuaboke.redisx.utils.FieldOrmUtil;
import com.dahuaboke.redisx.utils.YamlUtil;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Redisx {

    public static void main(String[] args) {
        Config config = new Config();
        FieldOrmUtil.MapToBean(YamlUtil.parseYamlParam(args),config);
        Controller controller = new Controller(config.getRedisVersion(), config.isFromIsCluster(), config.getFromPassword(),
                config.isToIsCluster(), config.getToPassword(), config.isImmediate(), config.getImmediateResendTimes(), config.getSwitchFlag());
        controller.start(config.getFromAddresses(), config.getToAddresses(), config.isConsoleEnable(),
                config.getConsolePort(), config.getConsoleTimeout(), config.isAlwaysFullSync(), config.isSyncRdb(), config.getToFlushSize());
    }

    public static class Config {

        @FieldOrm(value = "redisx.from.isCluster",defaultValue = "false")
        private boolean fromIsCluster;

        @FieldOrm(value = "redisx.from.address",required = true)
        private List<InetSocketAddress> fromAddresses;

        @FieldOrm(value = "redisx.from.password")
        private String fromPassword;

        @FieldOrm(value = "redisx.to.isCluster",defaultValue = "false")
        private boolean toIsCluster;

        @FieldOrm(value = "redisx.to.address",required = true)
        private List<InetSocketAddress> toAddresses;

        @FieldOrm(value = "redisx.to.password")
        private String toPassword;

        @FieldOrm(value = "redisx.to.flushSize",defaultValue = "50")
        private int toFlushSize;

        @FieldOrm(value = "redisx.console.enable",defaultValue = "false")
        private boolean consoleEnable;

        @FieldOrm(value = "redisx.console.timeout",defaultValue = "18080")
        private int consolePort;

        @FieldOrm(value = "redisx.console.timeout",defaultValue = "5000")
        private int consoleTimeout;

        @FieldOrm(value = "redisx.immediate.enable",defaultValue = "false")
        private boolean immediate;

        @FieldOrm(value = "redisx.alwaysFullSync",defaultValue = "false")
        private boolean alwaysFullSync;

        @FieldOrm(value = "redisx.immediate.resendTimes",defaultValue = "0")
        private int immediateResendTimes;

        @FieldOrm(value = "redisx.from.redis.version",required = true)
        private String redisVersion;

        @FieldOrm(value = "redisx.switchFlag",defaultValue = Constant.SWITCH_FLAG)
        private String switchFlag;

        @FieldOrm(value = "redisx.syncRdb",defaultValue = "true")
        private boolean syncRdb;

        public boolean isFromIsCluster() {
            return fromIsCluster;
        }

        public void setFromIsCluster(boolean fromIsCluster) {
            this.fromIsCluster = fromIsCluster;
        }

        public List<InetSocketAddress> getFromAddresses() {
            return fromAddresses;
        }

        public void setFromAddresses(List<String> fromAddressesStr) {
            List<InetSocketAddress> fromAddresses = new ArrayList();
            for (String address : fromAddressesStr) {
                String[] hostAndPortAry = address.split(":");
                InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAndPortAry[0], Integer.parseInt(hostAndPortAry[1]));
                fromAddresses.add(inetSocketAddress);
            }
            this.fromAddresses = fromAddresses;
        }

        public String getFromPassword() {
            return fromPassword;
        }

        public void setFromPassword(String fromPassword) {
            this.fromPassword = fromPassword;
        }

        public boolean isToIsCluster() {
            return toIsCluster;
        }

        public void setToIsCluster(boolean toIsCluster) {
            this.toIsCluster = toIsCluster;
        }

        public List<InetSocketAddress> getToAddresses() {
            return toAddresses;
        }

        public void setToAddresses(List<String> toAddressesStr) {
            List<InetSocketAddress> toAddresses = new ArrayList();
            for (String address : toAddressesStr) {
                String[] hostAndPortAry = address.split(":");
                InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAndPortAry[0], Integer.parseInt(hostAndPortAry[1]));
                toAddresses.add(inetSocketAddress);
            }
            this.toAddresses = toAddresses;
        }

        public String getToPassword() {
            return toPassword;
        }

        public void setToPassword(String toPassword) {
            this.toPassword = toPassword;
        }

        public int getToFlushSize() {
            return toFlushSize;
        }

        public void setToFlushSize(int toFlushSize) {
            this.toFlushSize = toFlushSize;
        }

        public boolean isConsoleEnable() {
            return consoleEnable;
        }

        public void setConsoleEnable(boolean consoleEnable) {
            this.consoleEnable = consoleEnable;
        }

        public int getConsolePort() {
            return consolePort;
        }

        public void setConsolePort(int consolePort) {
            this.consolePort = consolePort;
        }

        public int getConsoleTimeout() {
            return consoleTimeout;
        }

        public void setConsoleTimeout(int consoleTimeout) {
            this.consoleTimeout = consoleTimeout;
        }

        public boolean isImmediate() {
            return immediate;
        }

        public void setImmediate(boolean immediate) {
            this.immediate = immediate;
        }

        public boolean isAlwaysFullSync() {
            return alwaysFullSync;
        }

        public void setAlwaysFullSync(boolean alwaysFullSync) {
            this.alwaysFullSync = alwaysFullSync;
        }

        public int getImmediateResendTimes() {
            return immediateResendTimes;
        }

        public void setImmediateResendTimes(int immediateResendTimes) {
            this.immediateResendTimes = immediateResendTimes;
        }

        public String getRedisVersion() {
            return redisVersion;
        }

        public void setRedisVersion(String redisVersion) {
            this.redisVersion = redisVersion;
        }

        public String getSwitchFlag() {
            return switchFlag;
        }

        public void setSwitchFlag(String switchFlag) {
            this.switchFlag = switchFlag;
        }

        public boolean isSyncRdb() {
            return syncRdb;
        }

        public void setSyncRdb(boolean syncRdb) {
            this.syncRdb = syncRdb;
        }
    }

}