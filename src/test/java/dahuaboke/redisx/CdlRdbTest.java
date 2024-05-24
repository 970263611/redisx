package dahuaboke.redisx;

import com.dahuaboke.redisx.slave.rdb.RdbData;
import com.dahuaboke.redisx.slave.rdb.RdbParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;

public class CdlRdbTest {

    private static String root = "C:\\Users\\23195\\Desktop\\rdbfile\\";


    @Test
    public void rdbTest() throws Exception {
        String filename = "dump.rdb";
        FileInputStream inputStream = new FileInputStream(new File(root + filename));
        int length = inputStream.available();
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
        byteBuf.writeBytes(inputStream, length);
//        RdbParser parser = new RdbParser(byteBuf);
//        parser.parseHeader();
//        System.out.println(parser.getRdbInfo().getRdbHeader());
//        while (!parser.getRdbInfo().isEnd()) {
//            parser.parseData();
//            System.out.println("==============================");
//            System.out.println(parser.getRdbInfo().getRdbData());
//        }
//        System.out.println("end");
        parse(byteBuf);
    }

    private void parse(ByteBuf byteBuf) {
        RdbParser parser = new RdbParser(byteBuf);
        parser.parseHeader();
        System.out.println(parser.getRdbInfo().getRdbHeader());
        while (!parser.getRdbInfo().isEnd()) {
            parser.parseData();
            RdbData rdbData = parser.getRdbInfo().getRdbData();
            System.out.println(rdbData);
        }
    }

}
