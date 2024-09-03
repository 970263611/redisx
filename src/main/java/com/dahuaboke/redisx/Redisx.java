package com.dahuaboke.redisx;


import com.dahuaboke.redisx.common.Constants;
import com.dahuaboke.redisx.common.annotation.FieldOrm;
import com.dahuaboke.redisx.common.enums.Mode;
import com.dahuaboke.redisx.common.interfaces.FieldOrmCheck;
import com.dahuaboke.redisx.common.utils.FieldOrmUtil;
import com.dahuaboke.redisx.common.utils.StringUtils;
import com.dahuaboke.redisx.common.utils.YamlUtil;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Redisx {

    private static final Logger logger = LoggerFactory.getLogger(Redisx.class);

    public static void main(String[] args) {
        Config config = new Config();
        FieldOrmUtil.MapToBean(YamlUtil.parseYamlParam(args), config);
        Configurator.setRootLevel(Level.getLevel(config.getLogLevelGlobal()));
        Controller controller = new Controller(config);
        controller.start();
    }

    public static class Config implements FieldOrmCheck {

        @FieldOrm(value = "redisx.from.mode", required = true, setType = String.class)
        private Mode fromMode;

        @FieldOrm(value = "redisx.from.masterName")
        private String fromMasterName;

        @FieldOrm(value = "redisx.from.address", required = true)
        private List<InetSocketAddress> fromAddresses;

        @FieldOrm(value = "redisx.from.username")
        private String fromUsername;

        @FieldOrm(value = "redisx.from.password")
        private String fromPassword;

        @FieldOrm(value = "redisx.to.mode", required = true, setType = String.class)
        private Mode toMode;

        @FieldOrm(value = "redisx.to.masterName")
        private String toMasterName;

        @FieldOrm(value = "redisx.to.address", required = true)
        private List<InetSocketAddress> toAddresses;

        @FieldOrm(value = "redisx.to.username")
        private String toUsername;

        @FieldOrm(value = "redisx.to.password")
        private String toPassword;

        @FieldOrm(value = "redisx.to.flushSize", defaultValue = "50")
        private int toFlushSize;

        @FieldOrm(value = "redisx.console.enable", defaultValue = "false")
        private boolean consoleEnable;

        @FieldOrm(value = "redisx.console.port", defaultValue = "18080")
        private int consolePort;

        @FieldOrm(value = "redisx.console.timeout", defaultValue = "5000")
        private int consoleTimeout;

        @FieldOrm(value = "redisx.immediate.enable", defaultValue = "false")
        private boolean immediate;

        @FieldOrm(value = "redisx.alwaysFullSync", defaultValue = "false")
        private boolean alwaysFullSync;

        @FieldOrm(value = "redisx.immediate.resendTimes", defaultValue = "1")
        private int immediateResendTimes;

        @FieldOrm(value = "redisx.from.redis.version", required = true)
        private String redisVersion;

        @FieldOrm(value = "redisx.switchFlag", defaultValue = Constants.SWITCH_FLAG)
        private String switchFlag;

        @FieldOrm(value = "redisx.syncRdb", defaultValue = "true")
        private boolean syncRdb;

        @FieldOrm(value = "logging.level.global", defaultValue = "INFO")
        private String logLevelGlobal;

        @FieldOrm(value = "redisx.to.flushDb", defaultValue = "false")
        private boolean flushDb;

        @FieldOrm(value = "redisx.to.syncWithCheckSlot", defaultValue = "false")
        private boolean syncWithCheckSlot;

        @FieldOrm(value = "redisx.from.verticalScaling", defaultValue = "false")
        private boolean verticalScaling;

        @FieldOrm(value = "redisx.from.connectMaster", defaultValue = "false")
        private boolean connectMaster;

        @Override
        public void check() {
            if (Mode.SENTINEL == this.fromMode && StringUtils.isEmpty(this.fromMasterName)) {
                throw new IllegalArgumentException("redisx.from.masterName");
            }
            if (Mode.SENTINEL == this.toMode && StringUtils.isEmpty(this.toMasterName)) {
                throw new IllegalArgumentException("redisx.to.masterName");
            }
            //强制全量同步必须同步rdb文件
            if (this.isAlwaysFullSync()) {
                this.setSyncRdb(true);
            }
            if (StringUtils.isNotEmpty(this.fromPassword) && StringUtils.isNotEmpty(this.fromUsername)) {
                this.fromPassword = this.fromUsername + " " + this.fromPassword;
            }
            if (StringUtils.isNotEmpty(this.toPassword) && StringUtils.isNotEmpty(this.toUsername)) {
                this.toPassword = this.toUsername + " " + this.toPassword;
            }
            if (immediateResendTimes < 1) {
                immediateResendTimes = 1;
            }
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

        public String getLogLevelGlobal() {
            return logLevelGlobal;
        }

        public void setLogLevelGlobal(String logLevelGlobal) {
            this.logLevelGlobal = logLevelGlobal;
        }

        public boolean isFlushDb() {
            return flushDb;
        }

        public void setFlushDb(boolean flushDb) {
            this.flushDb = flushDb;
        }

        public boolean isSyncWithCheckSlot() {
            return syncWithCheckSlot;
        }

        public void setSyncWithCheckSlot(boolean syncWithCheckSlot) {
            this.syncWithCheckSlot = syncWithCheckSlot;
        }

        public Mode getFromMode() {
            return fromMode;
        }

        public void setFromMode(String fromMode) {
            this.fromMode = Mode.getModeByString(fromMode);
        }

        public Mode getToMode() {
            return toMode;
        }

        public void setToMode(String toMode) {
            this.toMode = Mode.getModeByString(toMode);
        }

        public String getToMasterName() {
            return toMasterName;
        }

        public void setToMasterName(String toMasterName) {
            this.toMasterName = toMasterName;
        }

        public String getFromMasterName() {
            return fromMasterName;
        }

        public void setFromMasterName(String fromMasterName) {
            this.fromMasterName = fromMasterName;
        }

        public boolean isVerticalScaling() {
            return verticalScaling;
        }

        public void setVerticalScaling(boolean verticalScaling) {
            this.verticalScaling = verticalScaling;
        }

        public boolean isConnectMaster() {
            return connectMaster;
        }

        public void setConnectMaster(boolean connectMaster) {
            this.connectMaster = connectMaster;
        }


    }
}