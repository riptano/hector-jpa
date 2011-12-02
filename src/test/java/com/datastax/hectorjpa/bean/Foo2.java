package com.datastax.hectorjpa.bean;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;

@Entity
@ColumnFamily("Foo2ColumnFamily")
@Index(fields = "stored")
@NamedQueries({ @NamedQuery(name = "searchRangeIncludeMinExcludeMaxWithLong",
        query = "select t from Foo2 as t where t.stored >= :otherLow and t.stored < :otherHigh")
})
public class Foo2 {
  
    @Id
    @GeneratedValue
    private Long id;
    private Long stored;

    public Foo2() {
    }

    public Foo2(Long stored) {
        this.stored = stored;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getStored() {
        return stored;
    }

    public void setStored(Long stored) {
        this.stored = stored;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((id == null) ? 0 : id.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (!(obj instanceof Foo2))
        return false;
      Foo2 other = (Foo2) obj;
      if (id == null) {
        if (other.id != null)
          return false;
      } else if (!id.equals(other.id))
        return false;
      return true;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("Foo2 [id=");
      builder.append(id);
      builder.append(", stored=");
      builder.append(stored);
      builder.append("]");
      return builder.toString();
    }
}
