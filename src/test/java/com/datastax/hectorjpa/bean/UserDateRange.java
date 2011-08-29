package com.datastax.hectorjpa.bean;

import javax.persistence.Column;
import javax.persistence.Entity;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;
import com.eaio.uuid.UUID;

@Entity
@ColumnFamily("UserDateRangeColumnFamily")
@Index(fields = "userId, start, end")
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

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("UserDateRange [userId=");
    builder.append(userId);
    builder.append(", start=");
    builder.append(start);
    builder.append(", end=");
    builder.append(end);
    builder.append(", getId()=");
    builder.append(getId());
    builder.append("]");
    return builder.toString();
  }

  

}