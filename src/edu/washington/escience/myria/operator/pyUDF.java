/**
 *
 */
package edu.washington.escience.myria.operator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * generic operator for applying a Python based UDF
 */
public class pyUDF extends UnaryOperator {
  /***/
  private static final long serialVersionUID = 1L;

  private String filename;
  public static final Schema SCHEMA = Schema.ofFields(Type.INT_TYPE, "UDF");

  /**
   * The buffer holding the results.
   */
  // private transient TupleBatchBuffer ans;

  /**
   * Evaluator that evaluates {@link #predicate}.
   */
  // private BooleanEvaluator evaluator;

  /**
   * 
   * @param child child operator that data is fetched from
   * @param emitExpressions expression that created the output
   */
  public pyUDF(final String filename, final Operator child) {
    super(child);
    if (null != filename) {
      this.filename = filename;
    }

  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException {
    // this function should get the the batch of the tuples
    // apply run the python script on this batch of tuples
    // since python UDF will be applied to each tuple at a time

    Operator child = getChild();

    if (child.eoi() || child.eos()) {
      return null;
    }

    TupleBatch tb = child.nextReady();
    if (tb == null) {
      return null;
    }

    int rows = tb.numTuples();
    // final Schema s = new Schema(ImmutableList.of(Type.INT_TYPE), ImmutableList.of("sum"));
    final TupleBatchBuffer output = new TupleBatchBuffer(SCHEMA);

    List<? extends Column<?>> inputColumns = tb.getDataColumns();

    for (int i = 0; i < rows; i++) {
      // get all columns pass them to python function

      int v1 = inputColumns.get(0).getInt(i);
      int v2 = inputColumns.get(1).getInt(i);

      output.putInt(0, evalPython(v1, v2));

    }

    return output.popAnyUsingTimeout();

  }

  private int evalPython(final long v1, final long v2) {

    ProcessBuilder pb = new ProcessBuilder("python", filename, "" + v1, "" + v2);
    try {
      Process p = pb.start();
      BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
      int ret = new Integer(in.readLine()).intValue();
      return ret;
    } catch (Exception e) {
      System.out.println(e);
    }
    return 0;

  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {
    // for now just check that the filename is not empty
    // init may be used to send the python code to all the
    // executors
    Preconditions.checkNotNull(filename);
    // use this to pickle? python function and send it along to the workers.

  }

  @Override
  public Schema generateSchema() {
    return SCHEMA;
  }

}
