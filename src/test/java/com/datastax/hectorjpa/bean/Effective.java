/**
 * 
 */
package com.datastax.hectorjpa.bean;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.MappedSuperclass;
import javax.persistence.PostLoad;
import javax.persistence.PrePersist;

import org.apache.openjpa.persistence.Persistent;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Simple class to handle ranges based on dates. The start date is when the
 * relationship became effective, then end date is when the relationship is no
 * longer valid
 * 
 * @author Todd Nine
 * 
 */
@Entity
@MappedSuperclass
public abstract class Effective extends AbstractEntity implements Comparable<Effective> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Persistent
	@Column(name = "start")
	private Long startSaved;

	protected transient DateTime start;
	
	@Persistent
	protected String startTimeZone;

	@Persistent
	@Column(name = "end")
	private Long endSaved;

	protected transient DateTime end;
	
	@Persistent
	protected String endTimeZone;

	public DateTime getStart() {
		return start;
	}

	public void setStart(DateTime start) {
		this.start = start;
	}

	public DateTime getEnd() {
		return end;
	}

	public void setEnd(DateTime end) {
		this.end = end;
	}

	public Long getStartSaved() {
		return startSaved;
	}

	public Long getEndSaved() {
		return endSaved;
	}

	/**
	 * Copies the Joda time object to a long
	 */
	@PrePersist
	public void preSave() {
		startSaved = start.getMillis();
		startTimeZone = start.getZone().getID();

		if (end == null) {
			endSaved = null;
			endTimeZone = null;
			return;
		}

		endSaved = end.getMillis();
		endTimeZone = end.getZone().getID();
	}

	/**
	 * Copies the longs into Joda time objects
	 */
	@PostLoad
	public void postLoad() {
		start = new DateTime(startSaved, DateTimeZone.forID(startTimeZone));

		if (endSaved != null) {
			end = new DateTime(endSaved, DateTimeZone.forID(endTimeZone));
		}
	}

	

	@Override
	public int compareTo(Effective other) {

		int compare = start.compareTo(other.getStart());
		
		if(compare != 0){
			return compare;
		}
		
		//we're null, our span extends beyond the other range
		if(end == null && other.getEnd() != null){
			return -1;
		}
		
		if(end != null && other.getEnd() == null){
			return 1;
		}
		
		return 0;
	}
}
