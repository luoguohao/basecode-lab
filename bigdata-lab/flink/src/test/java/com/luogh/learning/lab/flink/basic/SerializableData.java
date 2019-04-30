package com.luogh.learning.lab.flink.basic;

import java.text.SimpleDateFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SerializableData {

  private String a;
  private SimpleDateFormat simpleDateFormat;
}
