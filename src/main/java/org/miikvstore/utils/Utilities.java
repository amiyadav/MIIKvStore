package org.miikvstore.utils;

import com.alibaba.fastjson.JSONObject;
import org.miikvstore.model.command.AppendCommand;
import org.miikvstore.model.command.Command;
import org.miikvstore.model.command.CommandType;
import org.miikvstore.model.command.DeleteCommand;

/**
 * utility methods for this app
 */
public class Utilities {

    public static final String TYPE = "type";

    public static Command jsonToCommand(JSONObject value) {
        if (value.getString(TYPE).equals(CommandType.APPEND.name())) {
            return value.toJavaObject(AppendCommand.class);
        } else if (value.getString(TYPE).equals(CommandType.DELETE.name())) {
            return value.toJavaObject(DeleteCommand.class);
        }
        return null;
    }

}
