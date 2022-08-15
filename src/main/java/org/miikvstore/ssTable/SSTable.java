package org.miikvstore.ssTable;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.miikvstore.Constant;
import org.miikvstore.model.command.AppendCommand;
import org.miikvstore.model.command.Command;
import org.miikvstore.model.command.DeleteCommand;
import org.miikvstore.utils.LoggerUtil;
import org.miikvstore.utils.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Optional;
import java.util.TreeMap;

/**
 * SSTable holding information around on disk file/ segments, its meta and operation on SSTable file
 */
@Getter
@Setter
public class SSTable implements Closeable {
    private final Logger LOGGER = LoggerFactory.getLogger(SSTable.class);

    private TreeMap<String, Position> sparseIndex;
    private MetaTableIndexInfo metaTableIndexInfo;
    private RandomAccessFile tableFileStream;
    private String filePath;

    public SSTable(long partitionSize, String filePath) {
        this.sparseIndex = new TreeMap<>();
        this.metaTableIndexInfo = new MetaTableIndexInfo();
        // set segment size
        metaTableIndexInfo.setPartSize(partitionSize);
        this.filePath = filePath;
        try {
            tableFileStream = new RandomAccessFile(this.filePath, Constant.RW);
            tableFileStream.seek(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * create SSTable object instance from on disk file
     *
     * @param filePath file path to ssTable files
     * @return SSTable
     */
    public static SSTable createFromFile(String filePath) {
        SSTable ssTable = new SSTable(0, filePath);
        ssTable.restoreFromFile();
        return ssTable;
    }

    /**
     * Restore the  meta information from file
     */
    private void restoreFromFile() {
        try {
            //read meta information of ssTable
            MetaTableIndexInfo tableMetaInfo = MetaTableIndexInfo.readFromFile(tableFileStream);
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][tableMetaInfo]: {}", tableMetaInfo);
            //read sparse index
            byte[] indexBytes = new byte[(int) tableMetaInfo.getIndexLen()];
            tableFileStream.seek(tableMetaInfo.getIndexStart());
            tableFileStream.read(indexBytes);
            String indexStr = new String(indexBytes, StandardCharsets.UTF_8);
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][indexStr]: {}", indexStr);
            sparseIndex = JSONObject.parseObject(indexStr,
                    new TypeReference<TreeMap<String, Position>>() {
                    });
            this.metaTableIndexInfo = tableMetaInfo;
            LoggerUtil.debug(LOGGER, "[SsTable][restoreFromFile][sparseIndex]: {}", sparseIndex);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    public static SSTable createFromMemTable(String dirPath, long partitionSize,
                                             TreeMap<String, Command> tempMemTable) throws IOException {
        // first create SsTable
        SSTable ssTable = new SSTable(partitionSize, dirPath);
        ssTable.convertMemTableToSsTable(tempMemTable);
        return ssTable;
    }

    private void convertMemTableToSsTable(TreeMap<String, Command> memTable) throws IOException {
        JSONObject partitionData = new JSONObject(true);
        //update meta information
        metaTableIndexInfo.setDataStart(tableFileStream.getFilePointer());
        // extract entries in memtable
        for (Command command : memTable.values()) {
            // if command is append
            if (command instanceof AppendCommand) {
                AppendCommand appendCommand = (AppendCommand) command;
                partitionData.put(appendCommand.getKey(), appendCommand);
            }

            // if command is delete
            if (command instanceof DeleteCommand) {
                DeleteCommand deleteCommand = (DeleteCommand) command;
                partitionData.put(deleteCommand.getKey(), deleteCommand);
            }

            if (partitionData.size() >= metaTableIndexInfo.getPartSize()) {
                // create new SSTable file and put first entry in sparse index
                createNewSsTable(partitionData);
            }

        }
        // write the remaining partition data
        if (partitionData.size() > 0) {
            createNewSsTable(partitionData);
        }

        long dataPartLength = tableFileStream.getFilePointer() - metaTableIndexInfo.getDataStart();
        metaTableIndexInfo.setDataLen(dataPartLength);

        byte[] indexBytes = JSONObject.toJSONString(sparseIndex).getBytes(StandardCharsets.UTF_8);

        // write sparse index to same ssTable file
        metaTableIndexInfo.setIndexStart(tableFileStream.getFilePointer());
        tableFileStream.write(indexBytes);
        metaTableIndexInfo.setIndexLen(indexBytes.length);
        LoggerUtil.debug(LOGGER, "[SSTable][convertMemTableToSsTable][sparseIndex]: {}", sparseIndex);

        //save file index
        metaTableIndexInfo.writeToFile(tableFileStream);
        LoggerUtil.info(LOGGER, "[SsTable][convertMemTableToSsTable]: {},{}", filePath, metaTableIndexInfo);
    }

    private void createNewSsTable(JSONObject partitionData) throws IOException {
        byte[] bytes = JSONObject.toJSONBytes(partitionData);
        long start = tableFileStream.getFilePointer();
        tableFileStream.write(bytes);

        //Record the first key of the data segment into the sparse index
        Optional<String> startKey = partitionData.keySet().stream().findFirst();
        startKey.ifPresent(s -> sparseIndex.put(s, new Position(start, bytes.length)));
        partitionData.clear();
    }


    /**
     * Query by key
     *
     * @param key search key
     * @return command
     */
    public Command query(String key) {
        LinkedList<Position> sparseKeyPositionList = new LinkedList<>();

        Position nearestLowestPosition = null;
        Position nearestHigherPosition = null;
        //find the position of nearest lowest key before search key
        for (String sparseIndexKey : sparseIndex.keySet()) {
            if (sparseIndexKey.compareTo(key) < 0) {
                nearestLowestPosition = sparseIndex.get(sparseIndexKey);
            } else {
                nearestHigherPosition = sparseIndex.get(sparseIndexKey);
                break;
            }
        }
        if (nearestLowestPosition != null) {
            sparseKeyPositionList.add(nearestLowestPosition);
        }
        if (nearestHigherPosition != null) {
            sparseKeyPositionList.add(nearestHigherPosition);
        }
        if (sparseKeyPositionList.size() == 0) {
            return null;
        }

        LoggerUtil.debug(LOGGER, "[SsTable][query][sparseKeyPositionList]: {}", sparseKeyPositionList);

        Pair<Position, Position> sparseKeyPositionPair = ImmutablePair.of(sparseKeyPositionList.getFirst(),
                sparseKeyPositionList.getLast());
        long start = sparseKeyPositionPair.getLeft().getStart();
        long length;

        if (sparseKeyPositionPair.getLeft().equals(sparseKeyPositionPair.getRight())) {
            length = sparseKeyPositionPair.getLeft().getLength();
        } else {
            length = sparseKeyPositionPair.getRight().getStart() + sparseKeyPositionPair.getKey().getLength() - start;
        }

        //read the data in-between those bytes will result in reduced IO
        byte[] bytes = new byte[(int) length];
        try {
            tableFileStream.seek(start);
            tableFileStream.read(bytes);
            // read the data partition-wise
            // partition pointer
            int pPointer = 0;
            JSONObject partition = JSONObject.parseObject(
                    new String(bytes, pPointer, (int) sparseKeyPositionPair.getLeft().getLength()));
            LoggerUtil.debug(LOGGER, "[SsTable][query][partition]: {}", partition);
            if (partition.containsKey(key)) {
                JSONObject value = partition.getJSONObject(key);
                return Utilities.jsonToCommand(value);
            } else {
                if(!sparseKeyPositionPair.getLeft().equals(sparseKeyPositionPair.getRight())){

                    pPointer = pPointer + (int) sparseKeyPositionPair.getLeft().getLength();
                    partition = JSONObject.parseObject(
                            new String(bytes, pPointer, (int) sparseKeyPositionPair.getRight().getLength()));
                    if (partition.containsKey(key)) {
                        JSONObject value = partition.getJSONObject(key);
                        return Utilities.jsonToCommand(value);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        tableFileStream.close();
    }

}
