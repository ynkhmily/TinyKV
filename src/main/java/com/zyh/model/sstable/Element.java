package com.zyh.model.sstable;

import lombok.AllArgsConstructor;
import lombok.Data;


@Data
@AllArgsConstructor
public class Element {

    private String key;

    private String value;

    private Boolean deleted = false;

    public Element(String key, String value) {
        this.key = key;
        this.value = value;
    }
}
