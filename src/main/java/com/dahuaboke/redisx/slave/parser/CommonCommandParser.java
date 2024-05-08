package com.dahuaboke.redisx.slave.parser;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.redis.*;
import io.netty.util.ByteProcessor;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * 2024/5/8 10:59
 * auth: dahua
 * desc:
 */
public class CommonCommandParser implements CommandParser {

    private static final Logger logger = LoggerFactory.getLogger(CommonCommandParser.class);

    private final ToPositiveLongProcessor toPositiveLongProcessor = new ToPositiveLongProcessor();

    private final boolean decodeInlineCommands;
    private final int maxInlineMessageLength;
    private final RedisMessagePool messagePool;

    // current decoding states
    private State state = State.DECODE_TYPE;
    private RedisMessageType type;
    private int remainingBulkLength;

    static final int TYPE_LENGTH = 1;

    static final int EOL_LENGTH = 2;

    static final int NULL_LENGTH = 2;

    static final int NULL_VALUE = -1;

    static final int REDIS_MESSAGE_MAX_LENGTH = 512 * 1024 * 1024; // 512MB

    // 64KB is max inline length of current Redis server implementation.
    static final int REDIS_INLINE_MESSAGE_MAX_LENGTH = 64 * 1024;

    static final int POSITIVE_LONG_MAX_LENGTH = 19; // length of Long.MAX_VALUE

    static final int LONG_MAX_LENGTH = POSITIVE_LONG_MAX_LENGTH + 1; // +1 is sign

    static final short NULL_SHORT = makeShort('-', '1');

    static final short EOL_SHORT = makeShort('\r', '\n');

    static byte[] longToAsciiBytes(long value) {
        return Long.toString(value).getBytes(CharsetUtil.US_ASCII);
    }

    /**
     * Returns a {@code short} value using endian order.
     */
    static short makeShort(char first, char second) {
        return PlatformDependent.BIG_ENDIAN_NATIVE_ORDER ?
                (short) ((second << 8) | first) : (short) ((first << 8) | second);
    }

    /**
     * Returns a {@code byte[]} of {@code short} value. This is opposite of {@code makeShort()}.
     */
    static byte[] shortToBytes(short value) {
        byte[] bytes = new byte[2];
        if (PlatformDependent.BIG_ENDIAN_NATIVE_ORDER) {
            bytes[1] = (byte) ((value >> 8) & 0xff);
            bytes[0] = (byte) (value & 0xff);
        } else {
            bytes[0] = (byte) ((value >> 8) & 0xff);
            bytes[1] = (byte) (value & 0xff);
        }
        return bytes;
    }


    /**
     * Creates a new instance with default {@code maxInlineMessageLength} and {@code messagePool}
     * and inline command decoding disabled.
     */
    public CommonCommandParser() {
        this(false);
    }

    /**
     * Creates a new instance with default {@code maxInlineMessageLength} and {@code messagePool}.
     *
     * @param decodeInlineCommands if {@code true}, inline commands will be decoded.
     */
    public CommonCommandParser(boolean decodeInlineCommands) {
        this(REDIS_INLINE_MESSAGE_MAX_LENGTH, FixedRedisMessagePool.INSTANCE, decodeInlineCommands);
    }

    /**
     * Creates a new instance with inline command decoding disabled.
     *
     * @param maxInlineMessageLength the maximum length of inline message.
     * @param messagePool            the predefined message pool.
     */
    public CommonCommandParser(int maxInlineMessageLength, RedisMessagePool messagePool) {
        this(maxInlineMessageLength, messagePool, false);
    }

    /**
     * Creates a new instance.
     *
     * @param maxInlineMessageLength the maximum length of inline message.
     * @param messagePool            the predefined message pool.
     * @param decodeInlineCommands   if {@code true}, inline commands will be decoded.
     */
    public CommonCommandParser(int maxInlineMessageLength, RedisMessagePool messagePool, boolean decodeInlineCommands) {
        if (maxInlineMessageLength <= 0 || maxInlineMessageLength > REDIS_MESSAGE_MAX_LENGTH) {
            throw new RedisCodecException("maxInlineMessageLength: " + maxInlineMessageLength +
                    " (expected: <= " + REDIS_MESSAGE_MAX_LENGTH + ")");
        }
        this.maxInlineMessageLength = maxInlineMessageLength;
        this.messagePool = messagePool;
        this.decodeInlineCommands = decodeInlineCommands;
    }

    @Override
    public boolean matching(byte b) {
        return true;
    }

    private enum State {
        DECODE_TYPE,
        DECODE_INLINE, // SIMPLE_STRING, ERROR, INTEGER
        DECODE_LENGTH, // BULK_STRING, ARRAY_HEADER
        DECODE_BULK_STRING_EOL,
        DECODE_BULK_STRING_CONTENT,
    }

    @Override
    public List<RedisMessage> parse(ByteBuf in) {
        List<RedisMessage> out = new LinkedList();
        try {
            for (; ; ) {
                switch (state) {
                    case DECODE_TYPE:
                        if (!decodeType(in)) {
                            return out;
                        }
                        break;
                    case DECODE_INLINE:
                        if (!decodeInline(in, out)) {
                            return out;
                        }
                        break;
                    case DECODE_LENGTH:
                        if (!decodeLength(in, out)) {
                            return out;
                        }
                        break;
                    case DECODE_BULK_STRING_EOL:
                        if (!decodeBulkStringEndOfLine(in, out)) {
                            return out;
                        }
                        break;
                    case DECODE_BULK_STRING_CONTENT:
                        if (!decodeBulkStringContent(in, out)) {
                            return out;
                        }
                        break;
                    default:
                        throw new RedisCodecException("Unknown state: " + state);
                }
            }
        } catch (RedisCodecException e) {
            resetDecoder();
            throw e;
        } catch (Exception e) {
            resetDecoder();
            throw new RedisCodecException(e);
        }
    }

    private void resetDecoder() {
        state = State.DECODE_TYPE;
        remainingBulkLength = 0;
    }

    private boolean decodeType(ByteBuf in) throws Exception {
        if (!in.isReadable()) {
            return false;
        }

        type = RedisMessageType.readFrom(in, decodeInlineCommands);
        state = type.isInline() ? State.DECODE_INLINE : State.DECODE_LENGTH;
        return true;
    }

    private boolean decodeInline(ByteBuf in, List<RedisMessage> out) throws Exception {
        ByteBuf lineBytes = readLine(in);
        if (lineBytes == null) {
            if (in.readableBytes() > maxInlineMessageLength) {
                throw new RedisCodecException("length: " + in.readableBytes() +
                        " (expected: <= " + maxInlineMessageLength + ")");
            }
            return false;
        }
        out.add(newInlineRedisMessage(type, lineBytes));
        resetDecoder();
        return true;
    }

    private boolean decodeLength(ByteBuf in, List<RedisMessage> out) throws Exception {
        ByteBuf lineByteBuf = readLine(in);
        if (lineByteBuf == null) {
            return false;
        }
        final long length = parseRedisNumber(lineByteBuf);
        if (length < NULL_VALUE) {
            throw new RedisCodecException("length: " + length + " (expected: >= " + NULL_VALUE + ")");
        }
        switch (type) {
            case ARRAY_HEADER:
                out.add(new ArrayHeaderRedisMessage(length));
                resetDecoder();
                return true;
            case BULK_STRING:
                if (length > REDIS_MESSAGE_MAX_LENGTH) {
                    throw new RedisCodecException("length: " + length + " (expected: <= " +
                            REDIS_MESSAGE_MAX_LENGTH + ")");
                }
                remainingBulkLength = (int) length; // range(int) is already checked.
                return decodeBulkString(in, out);
            default:
                throw new RedisCodecException("bad type: " + type);
        }
    }

    private boolean decodeBulkString(ByteBuf in, List<RedisMessage> out) throws Exception {
        switch (remainingBulkLength) {
            case NULL_VALUE: // $-1\r\n
                out.add(FullBulkStringRedisMessage.NULL_INSTANCE);
                resetDecoder();
                return true;
            case 0:
                state = State.DECODE_BULK_STRING_EOL;
                return decodeBulkStringEndOfLine(in, out);
            default: // expectedBulkLength is always positive.
                out.add(new BulkStringHeaderRedisMessage(remainingBulkLength));
                state = State.DECODE_BULK_STRING_CONTENT;
                return decodeBulkStringContent(in, out);
        }
    }

    // $0\r\n <here> \r\n
    private boolean decodeBulkStringEndOfLine(ByteBuf in, List<RedisMessage> out) throws Exception {
        if (in.readableBytes() < EOL_LENGTH) {
            return false;
        }
        readEndOfLine(in);
        out.add(FullBulkStringRedisMessage.EMPTY_INSTANCE);
        resetDecoder();
        return true;
    }

    // ${expectedBulkLength}\r\n <here> {data...}\r\n
    private boolean decodeBulkStringContent(ByteBuf in, List<RedisMessage> out) throws Exception {
        final int readableBytes = in.readableBytes();
        if (readableBytes == 0 || remainingBulkLength == 0 && readableBytes < EOL_LENGTH) {
            return false;
        }

        // if this is last frame.
        if (readableBytes >= remainingBulkLength + EOL_LENGTH) {
            ByteBuf content = in.readSlice(remainingBulkLength);
            readEndOfLine(in);
            // Only call retain after readEndOfLine(...) as the method may throw an exception.
            out.add(new DefaultLastBulkStringRedisContent(content.retain()));
            resetDecoder();
            return true;
        }

        // chunked write.
        int toRead = Math.min(remainingBulkLength, readableBytes);
        remainingBulkLength -= toRead;
        out.add(new DefaultBulkStringRedisContent(in.readSlice(toRead).retain()));
        return true;
    }

    private static void readEndOfLine(final ByteBuf in) {
        final short delim = in.readShort();
        if (EOL_SHORT == delim) {
            return;
        }
        final byte[] bytes = shortToBytes(delim);
        throw new RedisCodecException("delimiter: [" + bytes[0] + "," + bytes[1] + "] (expected: \\r\\n)");
    }

    private RedisMessage newInlineRedisMessage(RedisMessageType messageType, ByteBuf content) {
        switch (messageType) {
            case INLINE_COMMAND:
                return new InlineCommandRedisMessage(content.toString(CharsetUtil.UTF_8));
            case SIMPLE_STRING: {
                SimpleStringRedisMessage cached = messagePool.getSimpleString(content);
                return cached != null ? cached : new SimpleStringRedisMessage(content.toString(CharsetUtil.UTF_8));
            }
            case ERROR: {
                ErrorRedisMessage cached = messagePool.getError(content);
                return cached != null ? cached : new ErrorRedisMessage(content.toString(CharsetUtil.UTF_8));
            }
            case INTEGER: {
                IntegerRedisMessage cached = messagePool.getInteger(content);
                return cached != null ? cached : new IntegerRedisMessage(parseRedisNumber(content));
            }
            default:
                throw new RedisCodecException("bad type: " + messageType);
        }
    }

    private static ByteBuf readLine(ByteBuf in) {
        if (!in.isReadable(EOL_LENGTH)) {
            return null;
        }
        final int lfIndex = in.forEachByte(ByteProcessor.FIND_LF);
        if (lfIndex < 0) {
            return null;
        }
        ByteBuf data = in.readSlice(lfIndex - in.readerIndex() - 1); // `-1` is for CR
        readEndOfLine(in); // validate CR LF
        return data;
    }

    private long parseRedisNumber(ByteBuf byteBuf) {
        final int readableBytes = byteBuf.readableBytes();
        final boolean negative = readableBytes > 0 && byteBuf.getByte(byteBuf.readerIndex()) == '-';
        final int extraOneByteForNegative = negative ? 1 : 0;
        if (readableBytes <= extraOneByteForNegative) {
            throw new RedisCodecException("no number to parse: " + byteBuf.toString(CharsetUtil.US_ASCII));
        }
        if (readableBytes > POSITIVE_LONG_MAX_LENGTH + extraOneByteForNegative) {
            throw new RedisCodecException("too many characters to be a valid RESP Integer: " +
                    byteBuf.toString(CharsetUtil.US_ASCII));
        }
        if (negative) {
            return -parsePositiveNumber(byteBuf.skipBytes(extraOneByteForNegative));
        }
        return parsePositiveNumber(byteBuf);
    }

    private long parsePositiveNumber(ByteBuf byteBuf) {
        toPositiveLongProcessor.reset();
        byteBuf.forEachByte(toPositiveLongProcessor);
        return toPositiveLongProcessor.content();
    }

    private static final class ToPositiveLongProcessor implements ByteProcessor {
        private long result;

        @Override
        public boolean process(byte value) throws Exception {
            if (value < '0' || value > '9') {
                throw new RedisCodecException("bad byte in number: " + value);
            }
            result = result * 10 + (value - '0');
            return true;
        }

        public long content() {
            return result;
        }

        public void reset() {
            result = 0;
        }
    }
}
