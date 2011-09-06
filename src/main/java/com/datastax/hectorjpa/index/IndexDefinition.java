package com.datastax.hectorjpa.index;

import java.util.Arrays;

/**
 * A meta holder for index definitions
 * 
 * @author Todd Nine
 * 
 */
public class IndexDefinition {

  private IndexField[] indexedFields;

  private IndexOrder[] orderFields;
  
  private IndexField[] fieldByName;
  
  public IndexDefinition(IndexField[] indexedFields, IndexOrder[] orderFields) {
    this.indexedFields = indexedFields;
    this.orderFields = orderFields;
    
    fieldByName = Arrays.copyOf(indexedFields, indexedFields.length);
    Arrays.sort(fieldByName);
  }

  /**
   * @return the indexedFields
   */
  public IndexField[] getIndexedFields() {
    return indexedFields;
  }

  /**
   * @return the orderFields
   */
  public IndexOrder[] getOrderFields() {
    return orderFields;
  }

  /**
   * Return the index of this field name in our field list.
   * 
   * Returns -1 if it does not exist
   * 
   * @param fieldName
   * @return
   */
  public int getIndex(String fieldName) {
    for (int i = 0; i < indexedFields.length; i++) {
      if (indexedFields[i].getName().equals(fieldName)) {
        return i;
      }
    }

    return -1;
  }



  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.deepHashCode(fieldByName);
    result = prime * result + Arrays.deepHashCode(orderFields);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof IndexDefinition))
      return false;
    IndexDefinition other = (IndexDefinition) obj;
    if (!Arrays.deepEquals(fieldByName, other.fieldByName))
      return false;
    if (!Arrays.deepEquals(orderFields, other.orderFields))
      return false;
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("IndexDefinition[")
    .append("indexedFields=")
    .append(Arrays.asList(indexedFields))
    .append(",orderedFields=")
    .append(Arrays.asList(orderFields))
    .append("]");
    return sb.toString();
  }

  
}
