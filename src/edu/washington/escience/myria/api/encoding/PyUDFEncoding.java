/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.PyUDF;

/**
 * 
 */
public class PyUDFEncoding extends UnaryOperatorEncoding<PyUDF> {

  @Required
  // public Expression argPredicate;
  public String filename;
  @Required
  public int[] columnIdx;

  @Override
  public PyUDF construct(final ConstructArgs args) {
    return new PyUDF(filename, columnIdx, null);
  }
}
