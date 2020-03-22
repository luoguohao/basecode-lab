package com.luogh.learning.lab.flink.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SubEvent extends Event {
    private String subName;

    public SubEvent(String name, long timestamp, String subName) {
        super(name, timestamp);
        this.subName = subName;
    }
}
