package com.luogh.learning.lab.flink.basic;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.junit.Assert;
import org.junit.Test;

public class SerializeApp {

  @Test
  public void test() {
    Kryo kryo = new Kryo();
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm");
    SerializableData data = new SerializableData("as", simpleDateFormat);
    ByteBufferOutput output = new ByteBufferOutput(100000);
    kryo.writeObject(output, data);
    Input input = new Input(output.toBytes());
    SerializableData back = kryo.readObject(input, SerializableData.class);

    Assert.assertEquals("as", back.getA());
    Assert.assertEquals(simpleDateFormat, back.getSimpleDateFormat());
  }

  @Test
  public void testFlink() throws Exception {
    TypeInformation<SerializableData> typeInformation = TypeInformation.of(SerializableData.class);
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    TypeSerializer<SerializableData> serializer = typeInformation.createSerializer(env.getConfig());

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm");
    SerializableData data = new SerializableData("as", simpleDateFormat);

    ByteArrayOutputStream ot = new ByteArrayOutputStream(1000);
    DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(ot);
    serializer.serialize(data, output);

    byte[] re = ot.toByteArray();
    Assert.assertTrue(re.length > 0);

    DataInputViewStreamWrapper intput = new DataInputViewStreamWrapper(
        new ByteArrayInputStream(re));
    SerializableData r = serializer.deserialize(intput);

    Assert.assertNotNull(r);
    Assert.assertEquals(simpleDateFormat, r.getSimpleDateFormat());
  }
}
