package com.luogh.learning.lab.flink.cep;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event implements Serializable {
    private String name;
    private long timestamp;
}
