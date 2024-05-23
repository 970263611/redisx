package dahuaboke.redisx;

import com.dahuaboke.redisx.slave.rdb.ParserManager;
import com.dahuaboke.redisx.slave.rdb.base.ListPackParser;
import com.dahuaboke.redisx.slave.rdb.base.StringParser;
import com.dahuaboke.redisx.slave.rdb.base.ZipListParser;
import com.dahuaboke.redisx.slave.rdb.hash.HashListPackParser;
import com.dahuaboke.redisx.slave.rdb.hash.HashZipListParser;
import com.dahuaboke.redisx.slave.rdb.list.ListQuickList2Parser;
import com.dahuaboke.redisx.slave.rdb.list.ListQuickListParser;
import com.dahuaboke.redisx.slave.rdb.module.Module;
import com.dahuaboke.redisx.slave.rdb.module.Module2Parser;
import com.dahuaboke.redisx.slave.rdb.module.ModuleParser;
import com.dahuaboke.redisx.slave.rdb.set.SetIntSetParser;
import com.dahuaboke.redisx.slave.rdb.set.SetListPackParser;
import com.dahuaboke.redisx.slave.rdb.set.SetParser;
import com.dahuaboke.redisx.slave.rdb.stream.Stream;
import com.dahuaboke.redisx.slave.rdb.stream.StreamListPacks2Parser;
import com.dahuaboke.redisx.slave.rdb.stream.StreamListPacks3Parser;
import com.dahuaboke.redisx.slave.rdb.stream.StreamListPacksParser;
import com.dahuaboke.redisx.slave.rdb.zset.ZSetEntry;
import com.dahuaboke.redisx.slave.rdb.zset.ZSetListPackParser;
import com.dahuaboke.redisx.slave.rdb.zset.ZSetZipListParser;
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
import java.util.*;

import static com.dahuaboke.redisx.Constant.MODULE_SET;

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

    //Stream
    StreamListPacksParser streamListPacksParser = new StreamListPacksParser();
    StreamListPacks2Parser streamListPacks2Parser = new StreamListPacks2Parser();
    StreamListPacks3Parser streamListPacks3Parser = new StreamListPacks3Parser();

    //Module
    Module2Parser module2Parser = new Module2Parser();

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
        byte[] bytes = stringParser.parse(byteBuf);
        String str = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(str);
    }

    @Test
    public void testListPack(){
        List<byte[]> bytes = listPackParser.parse(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }
    @Test
    public void testZipList(){
        List<byte[]> bytes = zipListParser.parse(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }

    @Test
    public void testQuickList(){
        List<byte[]> bytes = listQuickListParser.parse(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }

    @Test
    public void testQuickList2(){
        List<byte[]> bytes = listQuickList2Parser.parse(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }
    @Test
    public void testSet(){
        Set<byte[]> bytes = setParser.parse(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }
    @Test
    public void testSetIntSet(){
        Set<byte[]> bytes = setIntSetParser.parse(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }

    @Test
    public void testSetListPack(){
        Set<byte[]> bytes = setListPackParser.parse(byteBuf);
        bytes.forEach(entry -> {
            String str = new String(entry, StandardCharsets.UTF_8);
            System.out.println(str);
        });
    }
    @Test
    public void testZSetZipList(){
        Set<ZSetEntry> zSetEntries = zSetZipListParser.parse(byteBuf);
        zSetEntries.forEach(entry -> {
            double score =entry.getScore();
            String element = new String(entry.getElement(), StandardCharsets.UTF_8);
            System.out.println(score+":"+element);
        });
    }
    @Test
    public void testZSetListPack(){
        Set<ZSetEntry> zSetEntries = zSetListPackParser.parse(byteBuf);
        zSetEntries.forEach(entry -> {
            double score =entry.getScore();
            String element = new String(entry.getElement(), StandardCharsets.UTF_8);
            System.out.println(score+":"+element);
        });
    }

    @Test
    public void testHashZipList(){
        Map<byte[], byte[]> map = hashZipListParser.parse(byteBuf);
        Set<Map.Entry<byte[], byte[]>> entries = map.entrySet();
        entries.forEach(entry -> {
            String key = new String(entry.getKey(), StandardCharsets.UTF_8);
            String value = new String(entry.getValue(), StandardCharsets.UTF_8);
            System.out.println(key+":"+value);
        });
    }

    @Test
    public void testHashListPack(){
        Map<byte[], byte[]> map = hashListPackParser.parse(byteBuf);
        Set<Map.Entry<byte[], byte[]>> entries = map.entrySet();
        entries.forEach(entry -> {
            String key = new String(entry.getKey(), StandardCharsets.UTF_8);
            String value = new String(entry.getValue(), StandardCharsets.UTF_8);
            System.out.println(key+":"+value);
        });
    }

    @Test
    public void testStreamListPacksParser(){
        Stream stream = streamListPacksParser.parse(byteBuf);
        System.out.println(stream.toString());
    }
    @Test
    public void testStreamListPacksParser2(){
        Stream stream = streamListPacks2Parser.parse(byteBuf);
        System.out.println(stream.toString());
    }
    @Test
    public void testStreamListPacksParser3(){
        Stream stream = streamListPacks3Parser.parse(byteBuf);
        System.out.println(stream.toString());
    }

    @Test
    public void testVerseByte(){

        module2Parser.parse(byteBuf);

    }

}
