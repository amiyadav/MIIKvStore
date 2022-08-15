package org.miikvstore.ssTable;

import org.junit.Test;
import org.miikvstore.model.command.AppendCommand;
import org.miikvstore.model.command.Command;
import org.miikvstore.model.command.CommandType;
import org.miikvstore.model.command.DeleteCommand;

import java.io.IOException;
import java.util.TreeMap;

public class SSTableTest {

    @Test
    public void createFromIndex() throws IOException {
        TreeMap<String, Command> index = new TreeMap<>();
        for (int i = 0; i < 10; i++) {
            AppendCommand appendCommand = new AppendCommand("key" + i, "value" + i, CommandType.APPEND.name());
            index.put(appendCommand.getKey(), appendCommand);
        }
        index.put("key100", new AppendCommand("key100", "value100", CommandType.APPEND.name()));
        index.put("key100", new DeleteCommand("key100", CommandType.DELETE.name()));
        SSTable ssTable = SSTable.createFromMemTable("test.txt", 3, index);
        System.out.println(ssTable);
    }

    @Test
    public void query() {
        SSTable ssTable = SSTable.createFromFile("test.txt");
        System.out.println("1 %%%%" + ssTable.query("key0"));
        System.out.println("2 %%%%" + ssTable.query("key5"));
        System.out.println("3 %%%%" + ssTable.query("key9"));
        System.out.println("4 %%%%" + ssTable.query("key100"));
    }


}
