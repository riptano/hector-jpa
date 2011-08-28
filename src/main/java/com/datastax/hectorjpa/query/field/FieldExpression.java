package com.datastax.hectorjpa.query.field;

import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.UnsupportedException;

/**
 * Class to encapsulate an equality expression on a field. Does not care about
 * asc or descending. Start can be thought of as Value.MIN and end can be
 * thought of as Value.MAX
 * 
 * A < or <= expression will set the end value a = > or >= will set the start
 * value
 * 
 * @author Todd Nine
 * 
 */
public abstract class FieldExpression {

  protected FieldMetaData field;

  protected Operand startEquality;

  protected Object start;

  protected boolean startSet = false;
  
  protected Operand endEquality;

  // the end value in a range scan
  protected Object end;
  
  protected boolean endSet = false;
  

  public FieldExpression(FieldMetaData field) {
    this.field = field;

    startEquality = Operand.Equal;
    endEquality = Operand.Equal;

  }

  /**
   * Get the start value in a range scan
   * @return the start
   */
  public abstract Object getStart();

  /**
   * @param start
   *          the start to set
   * @param inclusive
   *          True if this is contains an equality operand I.E =, <=, >=
   */
  public void setStart(Object start, Operand equality) {
    if (this.startSet) {
      throw new UnsupportedException(
          String
              .format(
                  "You attempted to define the start value on field %s twice.  You must use the || operand to combine the use of the same operand on the same field with 2 values",
                  field));
    }

    this.start = start;
    this.startEquality = equality;
    this.startSet = true;
  }

  /**
   * Get the end value in a range scan
   * @return
   */
  public abstract Object getEnd();

  /**
   * @return the startEquality
   */
  public Operand getStartEquality() {
    return startEquality;
  }

  /**
   * @return the endEquality
   */
  public Operand getEndEquality() {
    return endEquality;
  }

  /**
   * @param end
   *          the end to set
   * @param inclusive
   *          True if this is contains an equality operand I.E =, <=, >=
   */
  public void setEnd(Object end, Operand equality) {
    if (this.endSet) {
      throw new UnsupportedException(
          String
              .format(
                  "You attempted to define the end value on field %s twice.  You must use the || operand to combine the use of the same operand on the same field with 2 values",
                  field));
    }
    this.end = end;
    this.endEquality = equality;
    this.endSet = true;
  }

  /**
   * @return the fieldName
   */
  public FieldMetaData getField() {
    return field;
  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("start=").append(start).append(",end=").append(end)
        .append(",startEquality=").append(startEquality)
        .append(",endEquality=").append(endEquality).append(",field=")
        .append(field.getName());
    return sb.toString();
  }


  /**
   * Used to keep our expressions correct
   * @author Todd Nine
   *
   */
  public enum Operand {
    LessThan,
    LessThanEqual,
    Equal,
    GreaterThanEqual,
    GreaterThan;
  }
}
