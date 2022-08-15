package org.miikvstore.core;

import com.alibaba.fastjson.JSONObject;
import org.miikvstore.Constant;
import org.miikvstore.model.command.AppendCommand;
import org.miikvstore.model.command.Command;
import org.miikvstore.model.command.CommandType;
import org.miikvstore.model.command.DeleteCommand;
import org.miikvstore.ssTable.SSTable;
import org.miikvstore.utils.Utilities;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Log structures merge Key value store standard methods implementation
 */
public class LSMKvStore implements KvStore {
    /**
     * In-Memory indexØ
     */
    private TreeMap<String, Command> memTable;

    /**
     * Temporary in-memory index
     */
    private TreeMap<String, Command> tempMemTable;

    /**
     * Directory path
     */
    private String dataDirPath;

    /**
     * lock
     */
    private ReadWriteLock readWriteLock;

    /**
     * in-memory index/memtable size threshold
     */
    private final long indexSizeThreshold;

    /**
     * partition size inside ssTable
     */
    private final long partitionSize;

    /**
     * File access stream
     */
    private RandomAccessFile writeAheadLog;

    /**
     * temporary data file
     */
    private File writeAheadLogFile;

    /**
     * ssTable list
     */
    private LinkedList<SSTable> ssTables;

    public LSMKvStore(String dataDirPath, int threshold, int partitionSize) throws FileNotFoundException {
        this.partitionSize = partitionSize;
        this.indexSizeThreshold = threshold;
        //init dir
        this.dataDirPath = dataDirPath;
        // init lock
        this.readWriteLock = new ReentrantReadWriteLock();
        // TODO change to skiplist
        this.ssTables = new LinkedList<>();
        this.memTable = new TreeMap<>();
        File fl = new File(dataDirPath);
        File[] files = fl.listFiles();
        // skip for hidden files too like .DS_Store etc
        if (files == null || files.length == 0 || (files.length == 1 && files[0].getName().startsWith("."))) {
            //If no files then create new index file and return
            writeAheadLogFile = new File(dataDirPath + Constant.WAL);
            writeAheadLog = new RandomAccessFile(writeAheadLogFile, Constant.RW);
            return;
        }

        //Load ssTable from largest to smallest
        TreeMap<Long, SSTable> ssTableTreeMap = new TreeMap<>(Comparator.reverseOrder());
        for (File file : files) {
            try {
                // if walTmp file is there that means something happened during writing index to sstable
                // we need to recover now
                if (file.isFile()) {
                    if (file.getName().equals(Constant.WAL_TMP)) {
                        recoverIndexFromWal(new RandomAccessFile(file, Constant.RW));

                    }
                    // prepare index from files
                    if (file.getName().endsWith(Constant.TABLE)) {
                        // extract timestamp
                        int dotIndex = file.getName().indexOf(".");
                        Long time = Long.parseLong(file.getName().substring(0, dotIndex));
                        // prepare SStable from SSTable file on disk
                        ssTableTreeMap.put(time, SSTable.createFromFile(file.getAbsolutePath()));
                    } else if (file.getName().endsWith(Constant.WAL)) {
                        //If no files then create new index file and return
                        writeAheadLogFile = new File(dataDirPath + Constant.WAL);
                        writeAheadLog = new RandomAccessFile(file, Constant.RW);
                        recoverIndexFromWal(writeAheadLog);
                    }
                }
            } catch (FileNotFoundException e) {
                System.out.println(" exception while finding file ==> " + e);
                e.printStackTrace();
            } catch (RuntimeException ex) {
                System.out.println(" Runtime exception ==> " + ex);
                ex.printStackTrace();
            }
        }
        ssTables.addAll(ssTableTreeMap.values());

    }

    private void recoverIndexFromWal(RandomAccessFile wal) {
        try {
            long length = wal.length();
            long start = 0;
            wal.seek(start);
            while (start < length) {
                int totalBytes = wal.readInt();
                byte[] bytes = new byte[totalBytes];
                wal.read(bytes);
                JSONObject data = JSONObject.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = Utilities.jsonToCommand(data);
                if (command != null) {
                    memTable.put(command.getKey(), command);
                }
                start += 4;
                start += totalBytes;
            }
            wal.seek(wal.length());
        } catch (RuntimeException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void append(String key, String value) {

        try {
            Command command = new AppendCommand(key, value, CommandType.APPEND.name());
            byte[] commandBytes = JSONObject.toJSONBytes(command);

            // acquire lock
            readWriteLock.writeLock().lock();

            // write to WAL before writing to index
            writeAheadLog.writeInt(commandBytes.length);
            writeAheadLog.write(commandBytes);

            // add in same memTable
            memTable.put(key, command);
            // check if index/memTable exists
            if (memTable.size() >= indexSizeThreshold) {
                prepareTmpIndex();
                flushToDiskSsTable();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            readWriteLock.writeLock().unlock();
        }

    }

    private void prepareTmpIndex() {
        //acquire write
        readWriteLock.writeLock().lock();
        // copy memTable to tmp memTable
        tempMemTable = memTable;
        // create new empty index/memTable
        memTable = new TreeMap<>();
        // stop WAL
        try {
            writeAheadLog.close();
            //delete existing tmp file if any
            File tmpWal = new File(dataDirPath + Constant.WAL_TMP);
            if (tmpWal.exists()) {
                if (!tmpWal.delete()) {
                    throw new RuntimeException("error while deleting the tmp wal file !!!");
                }
            }
            // Rename OLD WAL to tmpWAL
            if (!writeAheadLogFile.renameTo(tmpWal)) {
                throw new RuntimeException("error while renaming wal file !!!");
            }
            // Create New WAL file for new write to index
            writeAheadLogFile = new File(dataDirPath + Constant.WAL);
            writeAheadLog = new RandomAccessFile(writeAheadLogFile, Constant.RW);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            readWriteLock.writeLock().unlock();
        }

    }

    private void flushToDiskSsTable() throws IOException {
        //name ssTable incremental time
        //SSTable ssTable = new SSTable(partitionSize , dataDirPath);
        SSTable ssTable = SSTable.createFromMemTable(dataDirPath + System.currentTimeMillis() + Constant.TABLE,
                partitionSize, tempMemTable);
        ssTables.addFirst(ssTable);
        // delete temp index
        tempMemTable = null;
        // delete tmpWal
        File tmpWal = new File(dataDirPath + Constant.WAL_TMP);
        if (tmpWal.exists()) {
            if (!tmpWal.delete()) {
                throw new RuntimeException("Failed to delete file: walTmp");
            }
        }

    }


    @Override
    public String fetch(String key) {
        try {
            // lock while reading so
            readWriteLock.readLock().lock();
            // read from index/memTable first
            Command command = memTable.get(key);
            // check temporary memTable/ index
            if (null != tempMemTable && null == command) {
                command = tempMemTable.get(key);
            }
            // Go to SSTable
            if (command == null) {
                for (SSTable ssTable : ssTables) {
                    command = ssTable.query(key);
                    if (null != command) {
                        break;
                    }
                }

            }


            if (command instanceof AppendCommand) {
                return ((AppendCommand) command).getValue();
            }
            if (command instanceof DeleteCommand) {
                return null;
            }
            //Could not find description does not exist
            return null;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            readWriteLock.readLock().unlock();
        }
        return null;
    }

    @Override
    public void delete(String key) {
        try {
            //Delete and write operations are the same
            readWriteLock.writeLock().lock();
            DeleteCommand rmCommand = new DeleteCommand(key, CommandType.DELETE.name());
            byte[] commandBytes = JSONObject.toJSONBytes(rmCommand);
            writeAheadLog.writeInt(commandBytes.length);
            writeAheadLog.write(commandBytes);
            memTable.put(key, rmCommand);
            if (memTable.size() > indexSizeThreshold) {
                prepareTmpIndex();
                flushToDiskSsTable();
            }
        } catch (Throwable t) {
            t.printStackTrace();
            throw new RuntimeException(t);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        writeAheadLog.close();
        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }
}
