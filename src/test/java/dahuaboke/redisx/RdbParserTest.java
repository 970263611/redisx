package dahuaboke.redisx;

import com.dahuaboke.redisx.slave.rdb.ListPackParser;
import com.dahuaboke.redisx.slave.rdb.ZipListParser;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBuf;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @Desc: rdb解析测试类
 * @Author：zhh
 * @Date：2024/5/17 17:33
 */
public class RdbParserTest {
    ListPackParser listPackParser = new ListPackParser();
    ZipListParser zipListParser = new ZipListParser();

    @Test
    public void testListPack(){
        // 使用ByteBufAllocator来分配一个新的ByteBuf
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
        // 定义十六进制值对应的listPack byte数组
        byte[] hexValues = new byte[]{
                (byte) 0x11,(byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x04,(byte) 0x00,
                (byte) 0x81,(byte) 0x62,(byte) 0x02,
                (byte) 0x81,(byte) 0x61,(byte) 0x02,
                (byte) 0x02,(byte) 0x01,
                (byte) 0x01,(byte) 0x01,
                (byte) 0xFF
        };
        // 将byte数组写入ByteBuf
        byteBuf.writeBytes(hexValues);
        List<byte[]> bytes = listPackParser.parseListPack(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }
    @Test
    public void testZipList(){
        // 使用ByteBufAllocator来分配一个新的ByteBuf
        ByteBuf byteBuf = ByteBufAllocator.DEFAULT.buffer();
        // 定义十六进制值对应的ziplist byte数组
        byte[] hexValues = new byte[]{
                (byte) 0x15,(byte) 0x00,(byte) 0x00,(byte) 0x00,
                (byte) 0x12,(byte) 0x00,(byte)0x00,(byte) 0x00,
                (byte) 0x04,(byte) 0x00,
                (byte) 0x00,(byte) 0x01,(byte) 0x62,
                (byte) 0x03,(byte) 0x01,(byte) 0x61,
                (byte) 0x03,(byte) 0xF3,
                (byte) 0x02,(byte) 0xF2,
                (byte) 0xFF
        };
        // 将byte数组写入ByteBuf
        byteBuf.writeBytes(hexValues);
        List<byte[]> bytes = zipListParser.parseZipList(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }
}
