package com.datastax.hectorjpa.bean;

import javax.persistence.Column;
import javax.persistence.Entity;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;
import com.eaio.uuid.UUID;

@Entity
@ColumnFamily("UserDateRangeColumnFamily")
@Index(fields = "lastSaved,startSaved,userId")
public class UserDateRange extends AbstractEntity {


  @Persistent
  private UUID userId;

  @Persistent
  @Column(name = "start")
  private long start;

  @Persistent
  @Column(name = "end")
  private long end;


  public UUID getUserId() {
    return userId;
  }

  public void setUserId(UUID userId) {
    this.userId = userId;
  }

  public long getStart() {
    return start;
  }

  public void setStart(long start) {
    this.start = start;
  }

  public long getEnd() {
    return end;
  }

  public void setEnd(long end) {
    this.end = end;
  }

  

}