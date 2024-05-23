package com.dahuaboke.redisx.slave.rdb.module;

import com.dahuaboke.redisx.slave.rdb.ParserManager;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import io.netty.buffer.ByteBuf;

import static com.dahuaboke.redisx.Constant.*;

/**
 * @Desc:
 * @Author：zhh
 * @Date：2024/5/23 16:26
 */
public class Module2Parser implements Parser {

    @Override
    public Module parse(ByteBuf byteBuf) {
        char[] c = new char[9];
        long moduleId = ParserManager.LENGTH.parse(byteBuf).len;
        for (int i = 0; i < c.length; i++) {
            c[i] = MODULE_SET[(int) (moduleId >>> (10 + (c.length - 1 - i) * 6) & 63)];
        }
        String moduleName = new String(c);
        int moduleVersion = (int) (moduleId & 1023);
        //通过解析后的模板名称和模板版本,寻找模板解析器
        CustomModule2Parser moduleParser = this.findModuleParse(moduleName, moduleVersion);
        Module module = null;
        if (moduleParser == null) {
            //skip
            int opcode;
            while ((opcode = (int) ParserManager.LENGTH.parse(byteBuf).len) != RDB_MODULE_OPCODE_EOF) {
                if (opcode == RDB_MODULE_OPCODE_SINT || opcode == RDB_MODULE_OPCODE_UINT) {
                    ParserManager.LENGTH.parse(byteBuf);
                } else if (opcode == RDB_MODULE_OPCODE_STRING) {
                    ParserManager.STRING_00.parse(byteBuf);
                } else if (opcode == RDB_MODULE_OPCODE_FLOAT) {
                    byteBuf.skipBytes(4);
                } else if (opcode == RDB_MODULE_OPCODE_DOUBLE) {
                    byteBuf.skipBytes(8);
                }
            }
        } else {
            //customModule2Parser 开始解析,返回自定义Module对象
            module = moduleParser.parseModule(byteBuf, 2);
            long eof = ParserManager.LENGTH.parse(byteBuf).len;
            if (eof != RDB_MODULE_OPCODE_EOF) {
                throw new UnsupportedOperationException("The RDB file contains module data for the module '" + moduleName + "' that is not terminated by the proper module value EOF marker");
            }
        }
        return module;
    }

    public CustomModule2Parser findModuleParse(String moduleName, int moduleVersion) {
        //TODO 应该通过配置文件的方式,加载所有的模板,放入hashmap,key是moduleName+moduleVersion,value是CustomModule2Parser
        return null;
    }
}
