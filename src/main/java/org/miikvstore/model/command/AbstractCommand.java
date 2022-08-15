package org.miikvstore.model.command;

import com.alibaba.fastjson.JSON;
import lombok.Getter;
import lombok.Setter;

/**
 * Abstract command
 */
@Getter
@Setter
public abstract class AbstractCommand implements Command{



    private CommandType type;

    public AbstractCommand(CommandType type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

}
