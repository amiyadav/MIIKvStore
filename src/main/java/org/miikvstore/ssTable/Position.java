package org.miikvstore.ssTable;

import lombok.*;

@Data
@AllArgsConstructor
@Builder
public class Position {

    private long start;
    private long length;

}
