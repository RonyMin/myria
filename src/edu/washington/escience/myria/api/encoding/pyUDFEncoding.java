/**
 *
 */
package edu.washington.escience.myria.api.encoding;

import edu.washington.escience.myria.api.encoding.QueryConstruct.ConstructArgs;
import edu.washington.escience.myria.operator.pyUDF;

/**
 * 
 */
public class pyUDFEncoding extends UnaryOperatorEncoding<pyUDF> {

  @Required
  // public Expression argPredicate;
  public String filename;
  @Required
  public int[] columnIdx;

  @Override
  public pyUDF construct(final ConstructArgs args) {
    return new pyUDF(filename, columnIdx, null);
  }
}
