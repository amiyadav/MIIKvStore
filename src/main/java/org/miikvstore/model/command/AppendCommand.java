package org.miikvstore.model.command;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;

/**
 * Append new key value
 */
@Getter
@Setter
public class AppendCommand implements Command {

    private String key;
    private String value;
    private String type;

    public AppendCommand(String key, String value, String type) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
