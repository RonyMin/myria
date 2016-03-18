/**
 *
 */
package edu.washington.escience.myria.column;

import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

import edu.washington.escience.myria.Type;

/**
 * 
 */
public class BytesColumn extends Column<ByteBuffer> {
  /**
   * @param bs
   * @param i
   */
  public BytesColumn(final byte[] bs, final int i) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public ByteBuffer getBlob(final int index) {
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.washington.escience.myria.column.Column#getObject(int)
   */
  @Override
  public @Nonnull
  ByteBuffer getObject(final int row) {
    // TODO Auto-generated method stub
    return ByteBuffer.allocate(1);
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.washington.escience.myria.column.Column#getType()
   */
  @Override
  public Type getType() {
    // TODO Auto-generated method stub
    return null;
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.washington.escience.myria.column.Column#size()
   */
  @Override
  public int size() {
    // TODO Auto-generated method stub
    return 0;
  }
}
