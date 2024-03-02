package com.zyh.model.sstable;


import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class Position {
    public Long start;

    public Long len;

    public Boolean deleted;

}
