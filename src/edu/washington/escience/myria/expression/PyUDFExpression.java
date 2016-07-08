package edu.washington.escience.myria.expression;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.expression.evaluate.ExpressionOperatorParameter;

public class PyUDFExpression extends BinaryExpression {
  /***/
  private static final long serialVersionUID = 1L;
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(PyUDFExpression.class);

  /** The name of the python function. */
  @JsonProperty private final String name;
  @JsonProperty private final Type outputType;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  @SuppressWarnings("unused")
  private PyUDFExpression() {
    name = "";
    // picking python object as default output type.
    outputType = Type.BYTES_TYPE;
  }

  public PyUDFExpression(
      final ExpressionOperator left,
      final ExpressionOperator right,
      final String name,
      final Type outputType) {
    super(left, right);
    this.name = name; // python function name
    this.outputType = outputType; // python function output type
  }

  @Override
  public Type getOutputType(final ExpressionOperatorParameter parameters) {
    return outputType;
  }

  public String getName() {
    return name;
  }

  public Type getOutput() {
    return outputType;
  }

  // don't need the getJavaSubstring
  @Override
  public String getJavaString(final ExpressionOperatorParameter parameters) {
    return "";
  }
}
