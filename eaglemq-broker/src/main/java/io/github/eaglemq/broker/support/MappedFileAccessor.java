package io.github.eaglemq.broker.support;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * 使用 MMap 映射文件支持高性能读写
 */
public class MappedFileAccessor implements AutoCloseable {
    private final File file;
    private final MappedByteBuffer mappedByteBuffer;
    private final FileChannel fileChannel;

    /**
     * 构造函数，指定 offset 初始化文件映射
     *
     * @param filePath    文件路径
     * @param startOffset 起始偏移量
     * @param mappedSize  映射的内存体积
     * @throws Exception 如果文件不存在或映射失败
     */
    public MappedFileAccessor(String filePath, int startOffset, int mappedSize) throws Exception {
        file = new File(filePath);
        if (!file.exists()) {
            throw new FileNotFoundException("Invalid filePath: " + filePath);
        }
        fileChannel = new RandomAccessFile(file, "rw").getChannel();
        mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, startOffset, mappedSize);
    }

    /**
     * 从文件的指定偏移量开始读取内容
     *
     * @param readOffset 起始的读取偏移量
     * @param size       读取的内容的体积
     * @return 读取的字节数组
     */
    public byte[] read(int readOffset, int size) {
        mappedByteBuffer.position(readOffset);
        byte[] content = new byte[size];
        mappedByteBuffer.get(content, 0, size);
        return content;
    }

    /**
     * 以默认刷盘方式写入数据到文件中
     *
     * @param content 需要写入的数据内容
     */
    public void write(byte[] content) {
        this.write(content, false);
    }

    /**
     * 写入数据到文件中
     *
     * @param content 需要写入的数据内容
     * @param force   是否强制刷盘。false 代表不强制刷盘，默认先刷入 page cache，由 OS 控制落盘时间，是一种更高性能的做法
     */
    public void write(byte[] content, boolean force) {
        mappedByteBuffer.put(content);
        if (force) {
            mappedByteBuffer.force();
        }
    }

    /**
     * 释放 MMap 映射的内存，因为 JDK8 本身没有提供释放的方法，需要通过反射处理
     */
    public void clear() {
        if (mappedByteBuffer == null || !mappedByteBuffer.isDirect() || mappedByteBuffer.capacity() == 0) {
            return;
        }
        invoke(invoke(viewed(mappedByteBuffer), "cleaner"), "clean");
    }

    @Override
    public void close() throws Exception {
        if (fileChannel != null && fileChannel.isOpen()) {
            fileChannel.close();
        }
        clear();
    }

    private Object invoke(final Object target, final String methodName, final Class<?>... parameterTypes) {
        return AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                Method method = getMethod(target, methodName, parameterTypes);
                method.setAccessible(true);
                return method.invoke(target);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    private ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (Method method : methods) {
            if (method.getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    private Method getMethod(final Object target, final String name, final Class<?>... parameterTypes) throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(name, parameterTypes);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(name, parameterTypes);
        }
    }
}
