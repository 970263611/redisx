package dahuaboke.redisx;

import com.dahuaboke.redisx.slave.zhh.ListPackParser;
import com.dahuaboke.redisx.slave.zhh.StringParser;
import com.dahuaboke.redisx.slave.zhh.ZipListParser;
import com.dahuaboke.redisx.slave.zhh.hash.HashListPackParser;
import com.dahuaboke.redisx.slave.zhh.hash.HashParser;
import com.dahuaboke.redisx.slave.zhh.hash.HashZipListParser;
import com.dahuaboke.redisx.slave.zhh.list.ListParser;
import com.dahuaboke.redisx.slave.zhh.list.ListQuickList2Parser;
import com.dahuaboke.redisx.slave.zhh.list.ListQuickListParser;
import com.dahuaboke.redisx.slave.zhh.list.ListZipListParser;
import com.dahuaboke.redisx.slave.zhh.set.SetIntSetParser;
import com.dahuaboke.redisx.slave.zhh.set.SetListPackParser;
import com.dahuaboke.redisx.slave.zhh.set.SetParser;
import com.dahuaboke.redisx.slave.zhh.zset.ZSetEntry;
import com.dahuaboke.redisx.slave.zhh.zset.ZSetListPackParser;
import com.dahuaboke.redisx.slave.zhh.zset.ZSetZipListParser;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Desc: rdb解析测试类
 * @Author：zhh
 * @Date：2024/5/17 17:33
 */
public class RdbParserTest {

    //String
    StringParser stringParser = new StringParser();

    //Base
    ListPackParser listPackParser = new ListPackParser();
    ZipListParser zipListParser = new ZipListParser();

    //List
    ListQuickListParser listQuickListParser = new ListQuickListParser();
    ListQuickList2Parser listQuickList2Parser = new ListQuickList2Parser();

    //Set
    SetParser setParser = new SetParser();
    SetIntSetParser setIntSetParser = new SetIntSetParser();
    SetListPackParser setListPackParser = new SetListPackParser();

    //ZSet
    ZSetZipListParser zSetZipListParser = new ZSetZipListParser();
    ZSetListPackParser zSetListPackParser = new ZSetListPackParser();

    //Hash
    HashZipListParser hashZipListParser = new HashZipListParser();
    HashListPackParser hashListPackParser = new HashListPackParser();

    ByteBuf byteBuf = null;
    @Before
    public void initByteBuf()  {
        File file = new File("C:\\Users\\15536\\Desktop\\test.rdb");
        long fileLength = file.length();
        // 创建足够大小的ByteBuf来存储文件内容
        ByteBuf byteBuf = Unpooled.buffer(Math.toIntExact(fileLength));
        try (FileInputStream fis = new FileInputStream(file);
             FileChannel fileChannel = fis.getChannel()) {
            // 使用ByteBuffer作为临时缓冲区来从FileChannel中读取数据
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect((int) fileLength);
            // 读取文件内容到ByteBuffer
            while (fileChannel.read(byteBuffer) > 0) {
                // 准备缓冲区以进行读取（flip）
                byteBuffer.flip();
                // 将ByteBuffer的内容写入ByteBuf
                byteBuf.writeBytes(byteBuffer);
                // 清空ByteBuffer以便再次读取
                byteBuffer.clear();
            }
            // 确保所有内容都已写入ByteBuf
            byteBuf.writerIndex(byteBuf.capacity());
        } catch (IOException e) {
            // 如果发生异常，则释放ByteBuf
            byteBuf.release();
        }
        this.byteBuf = byteBuf;
    }

    @Test
    public void testLzfString(){
        byte[] bytes = stringParser.parseString(byteBuf);
        String str = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(str);
    }

    @Test
    public void testListPack(){
        List<byte[]> bytes = listPackParser.parseListPack(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }
    @Test
    public void testZipList(){
        List<byte[]> bytes = zipListParser.parseZipList(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }

    @Test
    public void testQuickList(){
        List<byte[]> bytes = listQuickListParser.parseQuickList(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }

    @Test
    public void testQuickList2(){
        List<byte[]> bytes = listQuickList2Parser.parseQuickList2(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }
    @Test
    public void testSet(){
        Set<byte[]> bytes = setParser.parseSet(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }
    @Test
    public void testSetIntSet(){
        Set<byte[]> bytes = setIntSetParser.parseSetIntSet(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }

    @Test
    public void testSetListPack(){
        Set<byte[]> bytes = setListPackParser.parseSetListPack(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }
    @Test
    public void testZSetZipList(){
        Set<ZSetEntry> zSetEntries = zSetZipListParser.parseZSetZipList(byteBuf);
        zSetEntries.forEach(entry -> {
            double score =entry.getScore();
            String element = new String(entry.getElement(), StandardCharsets.UTF_8);
            System.out.println(score+":"+element);
        });
    }
    @Test
    public void testZSetListPack(){
        Set<ZSetEntry> zSetEntries = zSetListPackParser.parseZSetListPack(byteBuf);
        zSetEntries.forEach(entry -> {
            double score =entry.getScore();
            String element = new String(entry.getElement(), StandardCharsets.UTF_8);
            System.out.println(score+":"+element);
        });
    }

    @Test
    public void testHashZipList(){
        Map<byte[], byte[]> map = hashZipListParser.parseHashZipList(byteBuf);
        Set<Map.Entry<byte[], byte[]>> entries = map.entrySet();
        entries.forEach(entry -> {
            String key = new String(entry.getKey(), StandardCharsets.UTF_8);
            String value = new String(entry.getValue(), StandardCharsets.UTF_8);
            System.out.println(key+":"+value);
        });
    }

    @Test
    public void testHashListPack(){
        Map<byte[], byte[]> map = hashListPackParser.parseHashListPack(byteBuf);
        Set<Map.Entry<byte[], byte[]>> entries = map.entrySet();
        entries.forEach(entry -> {
            String key = new String(entry.getKey(), StandardCharsets.UTF_8);
            String value = new String(entry.getValue(), StandardCharsets.UTF_8);
            System.out.println(key+":"+value);
        });
    }

}
