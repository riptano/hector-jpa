package com.datastax.hectorjpa.bean;

import com.datastax.hectorjpa.annotation.ColumnFamily;

import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.OneToMany;

@Entity
@ColumnFamily("ParentChildEntity")
public class ParentChildEntity {
  @Id
  @GeneratedValue
  private Long id;
  @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
  private List<ParentChildEntity> children;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public List<ParentChildEntity> getChildren() {
    return children;
  }

  public void setChildren(List<ParentChildEntity> children) {
    this.children = children;
  }
}
