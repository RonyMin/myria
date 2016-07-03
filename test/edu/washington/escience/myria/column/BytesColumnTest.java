/**
 *
 */
package edu.washington.escience.myria.column;

import static org.junit.Assert.assertTrue;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.junit.Test;

import edu.washington.escience.myria.column.builder.BytesColumnBuilder;
import edu.washington.escience.myria.column.builder.StringColumnBuilder;
import edu.washington.escience.myria.proto.DataProto.ColumnMessage;
import edu.washington.escience.myria.storage.TupleBatch;

public class BytesColumnTest {

  @Test
  public void testProto() {
    final BytesColumnBuilder original = new BytesColumnBuilder();
    original.appendByteBuffer(ByteBuffer.wrap(new String("ByteArray One").getBytes())).appendByteBuffer(
        ByteBuffer.wrap(new String("ByteArray Two").getBytes())).appendByteBuffer(
        ByteBuffer.wrap(new String("ByteArray Three").getBytes())).appendByteBuffer(
        ByteBuffer.wrap(new String("").getBytes())).appendByteBuffer(
        ByteBuffer.wrap(new String("ByteArray five").getBytes())).appendByteBuffer(
        ByteBuffer.wrap(new String("ByteArray Six").getBytes()));

    final ColumnMessage serialized = original.build().serializeToProto();
    final BytesColumn deserialized = BytesColumnBuilder.buildFromProtobuf(serialized, original.size());
    assertTrue(original.build().toString().equals(deserialized.toString()));
  }

  @Test
  public void testFull() {
    final BytesColumnBuilder builder = new BytesColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.appendByteBuffer(ByteBuffer.wrap(new String("One").getBytes()));
    }
    builder.build();
  }

  @Test(expected = BufferOverflowException.class)
  public void testOverflow() {
    final StringColumnBuilder builder = new StringColumnBuilder();
    for (int i = 0; i < TupleBatch.BATCH_SIZE; i++) {
      builder.appendByteBuffer(ByteBuffer.wrap(new String("false").getBytes()));
    }
    builder.appendByteBuffer(ByteBuffer.wrap(new String("true").getBytes()));
    builder.build();
  }
}
