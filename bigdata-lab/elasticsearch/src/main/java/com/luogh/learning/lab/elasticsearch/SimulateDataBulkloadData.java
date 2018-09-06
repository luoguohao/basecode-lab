package com.luogh.learning.lab.elasticsearch;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import com.alibaba.fastjson.JSON;
import java.io.File;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

@Slf4j
public class SimulateDataBulkloadData {

  private static final ThreadLocalRandom random = ThreadLocalRandom.current();

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addRequiredOption("r", "rows", true, "generate total records.");
    options.addOption("max", "maxDimensiion", true, "max dimension for one row data, default 1000");
    options.addOption("min", "minDimension", true, "min dimension for one row data, default 1");
    options.addRequiredOption("p", "outputPath", true, "target data file path");
    options.addOption("h", "help", false, "usage");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);
    if (cmd.hasOption("h")) {
      HelpFormatter hf = new HelpFormatter();
      hf.setWidth(110);
      hf.printHelp("simulate bulk load data", options, true);
    }

    int rows = Integer.parseInt(cmd.getOptionValue("r"));
    long maxDimension = Long.parseLong(cmd.getOptionValue("max", "1000"));
    long minDimension = Long.parseLong(cmd.getOptionValue("min", "1"));

    List<FieldType> fieldTypes = Arrays.asList(FieldType.values());
    File filePath = new File(cmd.getOptionValue("p"));
    System.out.println("generate json file:" + filePath);

    generate(rows, maxDimension, minDimension, fieldTypes, filePath.getPath());
  }


  /**
   * 生产模拟数据
   *
   * @param rows 数据记录条数
   * @param maxDimension 数据记录的最大维度
   * @param minDimension 数据记录的最小维度
   * @param supportedFieldTypes 字段支持的数据类型
   * @param outputFilePath 输出数据路径
   */
  public static void generate(int rows, long maxDimension, long minDimension,
      List<FieldType> supportedFieldTypes, String outputFilePath)
      throws Exception {
    final AtomicInteger consumedRow = new AtomicInteger(0);
    PrintWriter writer = new PrintWriter(new File(outputFilePath));
    IntStream.rangeClosed(0, rows)
        .mapToObj(id -> {
          long b = (maxDimension - minDimension) / 2;
          long a = Math.min(minDimension + (maxDimension - minDimension) / b, maxDimension); // 大部分是稀疏的数据

          long currentDimension = Math.min(Math.abs(Math.round(Math.sqrt(b) * random.nextGaussian() + a)), maxDimension); // 均值为a，方差为b的随机数

          Map<String, Object> dimensions = IntStream.range(0, (int) currentDimension)
              .parallel()
              .mapToObj(
                  index -> {
                    long randDimensionIndex = random.nextLong(minDimension, maxDimension);
                    FieldType fieldType = supportedFieldTypes.get((int)randDimensionIndex % FieldType.values().length);
                    return ImmutablePair.of(fieldType.fieldKeyPrefix + "_" + randDimensionIndex, fieldType.simulateField(randDimensionIndex));
                  })
              .collect(Collectors.toMap(ImmutablePair::getLeft, ImmutablePair::getRight, (k1, k2) -> k1));
          return new UserData(StringUtils.leftPad("0", 32), dimensions);
        })
        .forEach(userData -> {
          int currentRowNum = consumedRow.incrementAndGet();
          if (currentRowNum != 0 && currentRowNum % 2000 == 0) {
            log.info("already generate data:{}, remaining data size:{}, total rows: {}",
                currentRowNum, rows - currentRowNum, rows);
          }
          writer.println(JSON.toJSONString(userData));
        });

    writer.flush();
    writer.close();
  }

  @AllArgsConstructor
  public enum FieldType {
    Integer("int_key") {
      @Override
      public Object simulateField(long index) {
        return random.nextInt(0, 10000000);
      }
    },
    String("string_key") {
      @Override
      public Object simulateField(long index) {
        int randLength = random.nextInt(10, 40);
        return randString(randLength);
      }
    },
    Array("array_key") {
      @Override
      public Object simulateField(long index) {
        return IntStream.range(0, random.nextInt(20))
            .parallel()
            .mapToObj(x -> randString(10))
            .collect(toList());
      }
    },
    Date("date_key") {
      @Override
      public Object simulateField(long index) {
        return LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
      }
    },
    Boolean("boolean_key") {
      @Override
      public Object simulateField(long index) {
        return random.nextBoolean();
      }
    },
    Counter("counter_key") {
      @Override
      public Object simulateField(long index) {
        return IntStream.range(0, random.nextInt(20))
            .parallel()
            .mapToObj(id -> ImmutablePair.of("key_" + index + "_" + id, random.nextInt(1000)))
            .collect(toMap(ImmutablePair::getLeft, ImmutablePair::getRight));
      }
    };

    private final String fieldKeyPrefix;

    public abstract Object simulateField(long index);
  }


  public static String randString(int maxSize) {
    return IntStream.range(0, maxSize)
        .parallel()
        .mapToObj(x -> (char) random.nextInt(97, 122) + "")
        .collect(Collectors.joining());
  }


  @Data
  public static final class UserData {

    private final String userId;
    private final Map<String, Object> dimensions;
  }
}
