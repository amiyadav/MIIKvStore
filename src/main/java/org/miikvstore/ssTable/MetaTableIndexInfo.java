package org.miikvstore.ssTable;

import lombok.Getter;
import lombok.Setter;

import java.io.RandomAccessFile;

/**
 * SSTable and sparse index metadata information
 */
@Getter
@Setter
public class MetaTableIndexInfo {

    /**
     * version number
     */
    private long version;

    /**
     * data area start
     */
    private long dataStart;

    /**
     * data area length
     */
    private long dataLen;

    /**
     * start of index
     */
    private long indexStart;

    /**
     * index area length
     */
    private long indexLen;

    /**
     * Segment size
     */
    private long partSize;

    /**
     * write data to file
     * @param file file stream
     */
    public void writeToFile(RandomAccessFile file) {
        try {
            file.writeLong(partSize);
            file.writeLong(dataStart);
            file.writeLong(dataLen);
            file.writeLong(indexStart);
            file.writeLong(indexLen);
            file.writeLong(version);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    /**
     * Read SSTable meta information from the file
     * read it backwards in the order in which it was written
     *
     * @param file file stream
     * @return SSTable meta information
     */
    public static MetaTableIndexInfo readFromFile(RandomAccessFile file) {
        try {
            MetaTableIndexInfo MetaTableIndexInfo = new MetaTableIndexInfo();
            long fileLen = file.length();

            file.seek(fileLen - 8);
            MetaTableIndexInfo.setVersion(file.readLong());

            file.seek(fileLen - 8 * 2);
            MetaTableIndexInfo.setIndexLen(file.readLong());

            file.seek(fileLen - 8 * 3);
            MetaTableIndexInfo.setIndexStart(file.readLong());

            file.seek(fileLen - 8 * 4);
            MetaTableIndexInfo.setDataLen(file.readLong());

            file.seek(fileLen - 8 * 5);
            MetaTableIndexInfo.setDataStart(file.readLong());

            file.seek(fileLen - 8 * 6);
            MetaTableIndexInfo.setPartSize(file.readLong());

            return MetaTableIndexInfo;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }
}
