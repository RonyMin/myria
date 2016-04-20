/**
 *
 */
package edu.washington.escience.myria.operator;

import static org.junit.Assert.assertEquals;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.AndExpression;
import edu.washington.escience.myria.expression.Expression;
import edu.washington.escience.myria.expression.ExpressionOperator;
import edu.washington.escience.myria.expression.LessThanExpression;
import edu.washington.escience.myria.expression.MinusExpression;
import edu.washington.escience.myria.expression.PlusExpression;
import edu.washington.escience.myria.expression.VariableExpression;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;
import edu.washington.escience.myria.util.TestEnvVars;

/**
 * 
 */
public class PyUDFTest {
  @Test
  public void runSimpleScript() throws DbException {
    // One data point should be within the range, and the other is outside the range
    final Schema schema =
        new Schema(ImmutableList.of(Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE), ImmutableList.of("a", "b", "c"));
    final TupleBatchBuffer testBase = new TupleBatchBuffer(schema);
    String filename = "worker.py";
    // The middle case
    testBase.putInt(0, 4);
    testBase.putInt(1, 1);
    testBase.putInt(2, 4);
    // Way out of range
    testBase.putInt(0, 4);
    testBase.putInt(1, 1);
    testBase.putInt(2, 10);
    // Right at the edge, but shouldn't be included
    testBase.putInt(0, 4);
    testBase.putInt(1, 1);
    testBase.putInt(2, 3);

    File f = new File(filename);
    if (!f.exists()) {
      try {
        String prg = "import sys\nprint int(sys.argv[1])+int(sys.argv[2])\n";
        BufferedWriter out = new BufferedWriter(new FileWriter("test.py"));
        out.write(prg);
        out.close();
      } catch (Exception e) {
        System.out.println(e);
      }
    }
    int[] columnidx = new int[1];
    columnidx[0] = 0;
    PyUDF PyUDF = new PyUDF(filename, columnidx, new TupleSource(testBase));

    // getRowCount(pyUDF);
    // Filter filter = new Filter(WithinSumRangeExpression(0, 1, 2), new TupleSource(testBase));
    assertEquals(3, getRowCount(PyUDF));
  }

  private static int getRowCount(final Operator operator) throws DbException {
    operator.open(TestEnvVars.get());
    int count = 0;
    TupleBatch tb = null;
    while (!operator.eos()) {
      tb = operator.nextReady();
      if (tb != null) {
        count += tb.numTuples();
      }
    }
    return count;
  }

  private static Expression WithinSumRangeExpression(final int x, final int y, final int target) {
    ExpressionOperator varX = new VariableExpression(x);
    ExpressionOperator varY = new VariableExpression(y);
    ExpressionOperator varTarget = new VariableExpression(target);
    ExpressionOperator lower = new LessThanExpression(new MinusExpression(varX, varY), varTarget);
    ExpressionOperator upper = new LessThanExpression(varTarget, new PlusExpression(varX, varY));
    return new Expression("withinSumRange(" + x + "," + y + "," + target + ")", new AndExpression(lower, upper));
  }
}
