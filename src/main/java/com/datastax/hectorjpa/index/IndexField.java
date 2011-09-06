/**
 * 
 */
package com.datastax.hectorjpa.index;

import java.io.Serializable;

/**
 * Order element or cassandra ordering
 * 
 * @author Todd Nine
 * 
 */
public class IndexField implements Comparable<IndexField>, Serializable {

	/**
   * 
   */
	private static final long serialVersionUID = -2234285177676920926L;

	private String fieldName;

	public IndexField(String fieldName) {
		this.fieldName = fieldName;
	}

	public String getName() {
		return fieldName;
	}

	

	@Override
	public int compareTo(IndexField o) {
		if (o == null) {
			return 1;
		}

		return this.fieldName.compareTo(o.getName());
	}



	@Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (!(obj instanceof IndexField))
      return false;
    IndexField other = (IndexField) obj;
    if (fieldName == null) {
      if (other.fieldName != null)
        return false;
    } else if (!fieldName.equals(other.fieldName))
      return false;
    return true;
  }

  @Override
	public String toString() {
		return "FieldOrder [fieldName=" + fieldName + "]";
	}

}
