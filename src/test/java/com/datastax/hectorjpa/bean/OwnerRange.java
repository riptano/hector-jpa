/**
 * 
 */
package com.datastax.hectorjpa.bean;

import javax.persistence.Entity;

import org.apache.openjpa.persistence.Persistent;

import com.datastax.hectorjpa.annotation.ColumnFamily;
import com.datastax.hectorjpa.annotation.Index;
import com.datastax.hectorjpa.annotation.Indexes;
import com.eaio.uuid.UUID;

/**
 * Class to store the effective range for when a customer owned a spider
 * 
 * @author Todd Nine
 *
 */
@Entity
@ColumnFamily("OwnerRange")
@Indexes({
@Index(fields="userId", order="startSaved desc"),
//required for start date scanning
@Index(fields="userId, startSaved"),
//required for end date scanning
@Index(fields="userId, endSaved")
})
public class OwnerRange extends Effective {

	@Persistent
	private UUID userId;
	

	public UUID getUserId() {
		return userId;
	}

	public void setUserId(UUID userId) {
		this.userId = userId;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SpiderOwner [userId=");
		builder.append(userId);
		builder.append(", start=");
		builder.append(start);
		builder.append(", end=");
		builder.append(end);
		builder.append("]");
		return builder.toString();
	}
	

	
}
