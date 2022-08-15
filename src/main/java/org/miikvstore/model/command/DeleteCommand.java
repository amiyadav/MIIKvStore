package org.miikvstore.model.command;

import lombok.Getter;
import lombok.Setter;

/**
 * Delete command
 */
@Getter
@Setter
public class DeleteCommand implements Command {

    private String key;
    private String type;

    public DeleteCommand(String key, String type) {
        this.key = key;
        this.type = type;
    }

}
