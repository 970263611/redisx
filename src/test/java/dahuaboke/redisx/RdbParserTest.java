package dahuaboke.redisx;

import com.dahuaboke.redisx.slave.zhh.ListPackParser;
import com.dahuaboke.redisx.slave.zhh.ZipListParser;
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

/**
 * @Desc: rdb解析测试类
 * @Author：zhh
 * @Date：2024/5/17 17:33
 */
public class RdbParserTest {

    ListPackParser listPackParser = new ListPackParser();
    ZipListParser zipListParser = new ZipListParser();

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
}
