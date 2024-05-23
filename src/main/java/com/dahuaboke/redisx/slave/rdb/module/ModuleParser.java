package com.dahuaboke.redisx.slave.rdb.module;

import com.dahuaboke.redisx.slave.rdb.ParserManager;
import com.dahuaboke.redisx.slave.rdb.base.Parser;
import io.netty.buffer.ByteBuf;

import java.util.NoSuchElementException;

import static com.dahuaboke.redisx.Constant.MODULE_SET;

/**
 * @Desc:
 * @Author：zhh
 * @Date：2024/5/23 15:46
 */
public class ModuleParser implements Parser {

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
        CustomModuleParser moduleParser = this.findModuleParse(moduleName, moduleVersion);
        Module module = null;
        if (moduleParser == null) {
            throw new NoSuchElementException("module parser[" + moduleName + ", " + moduleVersion + "] not register. rdb type: [RDB_TYPE_MODULE]");
        } else {
            //customModuleParser 开始解析,返回自定义Module对象
            module = moduleParser.parseModule(byteBuf, 1);
        }
        return module;
    }

    public CustomModuleParser findModuleParse(String moduleName, int moduleVersion) {
        //TODO 应该通过配置文件的方式,加载所有的模板,放入hashmap,key是moduleName+moduleVersion,value是CustomModuleParser
        return null;
    }
}
