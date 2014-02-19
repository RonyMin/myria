package edu.washington.escience.myria.expression;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.SimplePredicate;
import edu.washington.escience.myria.Type;

/**
 * An ExpressionOperator with two children.
 */
public abstract class BinaryExpression extends ExpressionOperator {

  /***/
  private static final long serialVersionUID = 1L;

  /** The left child. */
  @JsonProperty
  private final ExpressionOperator left;
  /** The right child. */
  @JsonProperty
  private final ExpressionOperator right;

  /**
   * This is not really unused, it's used automagically by Jackson deserialization.
   */
  protected BinaryExpression() {
    left = null;
    right = null;
  }

  /**
   * @param left the left child.
   * @param right the right child.
   */
  protected BinaryExpression(final ExpressionOperator left, final ExpressionOperator right) {
    this.left = left;
    this.right = right;
  }

  /**
   * @return the left child;
   */
  public final ExpressionOperator getLeft() {
    return left;
  }

  /**
   * @return the right child;
   */
  public final ExpressionOperator getRight() {
    return right;
  }

  @Override
  public List<ExpressionOperator> getChildren() {
    ImmutableList.Builder<ExpressionOperator> children = ImmutableList.builder();
    return children.add(getLeft()).add(getRight()).build();
  }

  /**
   * Returns the infix binary string: "(" + left + infix + right + ")". E.g, for {@link PlusExpression},
   * <code>infix</code> is <code>"+"</code>.
   * 
   * @param infix the string representation of the Operator.
   * @param schema the input schema
   * @param stateSchema the schema of the state
   * @return the Java string for this operator.
   */
  protected final String getInfixBinaryString(final String infix, final Schema schema, final Schema stateSchema) {
    return new StringBuilder("(").append(getLeft().getJavaString(schema, stateSchema)).append(infix).append(
        getRight().getJavaString(schema, stateSchema)).append(')').toString();
  }

  /**
   * Returns the object comparison string: right + ".compareTo(" + left + ")" + op + "0". E.g, for
   * {@link EqualsExpression}, <code>op</code> is <code>LIKE</code> and <code>value</code> is <code>0</code>.
   * 
   * @param op integer comparison operator >, <, ==, >=, <=.
   * @param schema the input schema
   * @param stateSchema the schema of the state
   * @return the Java string for this operator.
   */
  protected final String getObjectComparisonString(final SimplePredicate.Op op, final Schema schema,
      final Schema stateSchema) {
    return new StringBuilder("(").append(getRight().getJavaString(schema, stateSchema)).append(".compareTo(").append(
        getLeft().getJavaString(schema, stateSchema)).append(')').append(op.toJavaString()).append(0).append(")")
        .toString();
  }

  /**
   * Returns the function call binary string: functionName + '(' + left + ',' + right + ")". E.g, for
   * {@link PowExpression}, <code>functionName</code> is <code>"Math.pow"</code>.
   * 
   * @param functionName the string representation of the Java function name.
   * @param schema the input schema
   * @param stateSchema the schema of the state
   * @return the Java string for this operator.
   */
  protected final String getFunctionCallBinaryString(final String functionName, final Schema schema,
      final Schema stateSchema) {
    return new StringBuilder(functionName).append('(').append(getLeft().getJavaString(schema, stateSchema)).append(',')
        .append(getRight().getJavaString(schema, stateSchema)).append(')').toString();
  }

  /**
   * A function that could be used as the default hash code for a binary expression.
   * 
   * @return a hash of (getClass().getCanonicalName(), left, right).
   */
  protected final int defaultHashCode() {
    return Objects.hash(getClass().getCanonicalName(), left, right);
  }

  @Override
  public int hashCode() {
    return defaultHashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !getClass().equals(other.getClass())) {
      return false;
    }
    BinaryExpression otherExpr = (BinaryExpression) other;
    return Objects.equals(left, otherExpr.left) && Objects.equals(right, otherExpr.right);
  }

  /**
   * A function that could be used as the default type checker for a binary expression where both operands must be
   * numeric.
   * 
   * @param schema the schema of the input tuples.
   * @param stateSchema the schema of the state
   * @return the default numeric type, based on the types of the children and Java type precedence.
   */
  protected final Type checkAndReturnDefaultNumericType(final Schema schema, final Schema stateSchema) {
    return checkAndReturnDefaultNumericType(schema, stateSchema, ImmutableList.of(Type.DOUBLE_TYPE, Type.FLOAT_TYPE,
        Type.LONG_TYPE, Type.INT_TYPE));
  }

  /**
   * A function that could be used as the default type checker for a binary expression where both operands must be
   * numeric.
   * 
   * @param schema the schema of the input tuples.
   * @param stateSchema the schema of the state
   * @param validTypes a list of valid types ordered by their precedence
   * @return the default numeric type, based on the types of the children and Java type precedence.
   */
  protected final Type checkAndReturnDefaultNumericType(final Schema schema, final Schema stateSchema,
      final List<Type> validTypes) {
    Type leftType = getLeft().getOutputType(schema, stateSchema);
    Type rightType = getRight().getOutputType(schema, stateSchema);
    int leftIdx = validTypes.indexOf(leftType);
    int rightIdx = validTypes.indexOf(rightType);
    Preconditions.checkArgument(leftIdx != -1, "%s cannot handle left child [%s] of Type %s", getClass()
        .getSimpleName(), getLeft(), leftType);
    Preconditions.checkArgument(rightIdx != -1, "%s cannot handle right child [%s] of Type %s", getClass()
        .getSimpleName(), getRight(), rightType);
    return validTypes.get(Math.min(leftIdx, rightIdx));
  }

  /**
   * A function that could be used as the default type checker for a binary expression where both operands must be
   * boolean.
   * 
   * @param schema the schema of the input tuples.
   * @param stateSchema the schema of the state
   */
  protected final void checkBooleanType(final Schema schema, final Schema stateSchema) {
    Type leftType = getLeft().getOutputType(schema, stateSchema);
    Type rightType = getRight().getOutputType(schema, stateSchema);
    Preconditions.checkArgument(leftType == Type.BOOLEAN_TYPE, "%s cannot handle left child [%s] of Type %s",
        getClass().getSimpleName(), getLeft(), leftType);
    Preconditions.checkArgument(rightType == Type.BOOLEAN_TYPE, "%s cannot handle right child [%s] of Type %s",
        getClass().getSimpleName(), getRight(), rightType);
  }
}
