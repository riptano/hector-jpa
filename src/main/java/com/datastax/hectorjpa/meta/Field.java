/**
 * 
 */
package com.datastax.hectorjpa.meta;

import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;


/**
 * Base class for field serialization logic
 * 
 * @author Todd Nine
 *
 */
public abstract class Field<V> {
  
  protected int fieldId;
  protected String name;

  
  public Field(int fieldId, String name){
    this.fieldId = fieldId;
    this.name = name;
  }
  

  /**
   * @return the fieldId
   */
  public int getFieldId() {
    return fieldId;
  }

  
  /**
   * @return the name
   */
  public String getName() {
    return name;
  }


  /**
   * Add this field the the mutator
   * @param stateManager
   * @param mutator
   * @param clock
   * @param key
   * @param cfName
   */
  public abstract void addField(OpenJPAStateManager stateManager, Mutator<byte[]> mutator, long clock, byte[] key, String cfName);

  
  
  @Override
  public String toString() {  
    return String.format("Field(fieldId: %d)", fieldId);
  }
  
 
  
}
