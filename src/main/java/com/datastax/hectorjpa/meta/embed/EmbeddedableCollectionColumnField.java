package com.datastax.hectorjpa.meta.embed;

import static com.datastax.hectorjpa.serializer.CompositeUtils.newComposite;

import java.util.Collection;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;

import com.datastax.hectorjpa.meta.StringColumnField;
import com.datastax.hectorjpa.service.IndexQueue;

/**
 * Class for serialising a single Embedded entity to a column value
 * 
 * @author Todd Nine
 * 
 * @param <V>
 */
public class EmbeddedableCollectionColumnField extends StringColumnField {

  protected static final DynamicCompositeSerializer serializer = new DynamicCompositeSerializer();
  
  protected static final IntegerSerializer intSerializer = IntegerSerializer.get();

  protected final FieldMetaData embeddedField;

  private final EmbeddedEntityValue entityValue;

  public EmbeddedableCollectionColumnField(FieldMetaData fmd, ClassMetaData elementMetaData) {
    super(fmd.getIndex(), fmd.getName());

    embeddedField = fmd;
        
    entityValue = new EmbeddedEntityValue(elementMetaData);

  }

  /**
   * Adds this field to the mutation with the given clock
   * 
   * @param stateManager
   * @param mutator
   * @param clock
   * @param key
   *          The row key
   * @param cfName
   *          the column family name
   */
  public void addField(OpenJPAStateManager stateManager,
      Mutator<byte[]> mutator, long clock, byte[] key, String cfName,
      IndexQueue queue) {

    Object value = stateManager.fetch(fieldId);

    if (value == null) {
      mutator.addDeletion(key, cfName, name, StringSerializer.get(), clock);
      return;
    }
    
    DynamicComposite c = newComposite();
    
    int size = ((Collection<?>)value).size();
    
    c.addComponent(size, intSerializer);
    
    //Write all values to the composite
    for(Object element: (Collection<?>) value){
      OpenJPAStateManager em = stateManager.getContext().getStateManager(element);
      entityValue.writeToComposite(em, c);
    }

    mutator.addInsertion(key, cfName, new HColumnImpl<String, DynamicComposite>(name, c, clock,
        StringSerializer.get(), serializer));

  }

  /**
   * Read the field from the query result into the opject within the state
   * manager.
   * 
   * @param stateManager
   * @param result
   * @return True if the field was loaded. False otherwise
   */
  @SuppressWarnings("unchecked")
  public boolean readField(OpenJPAStateManager stateManager,
      QueryResult<ColumnSlice<String, byte[]>> result) {

    HColumn<String, byte[]> column = result.get().getColumnByName(name);

    if (column == null) {
      return false;
    }

    DynamicComposite composite = serializer.fromBytes(column.getValue());
    
    Collection<Object> collection = (Collection<Object>) stateManager.newFieldProxy(fieldId);
    
    int size = composite.get(0, intSerializer);
    
    int startIndex = 1;
    
    for(int i = 0; i < size; i ++){
      OpenJPAStateManager embeddedSm = stateManager.getContext().embed(null, null, stateManager, embeddedField.getElement());
      
      startIndex = entityValue.getFromComposite(embeddedSm, composite, startIndex);
      
      collection.add(embeddedSm.getManagedInstance());
      
    }

    stateManager.store(fieldId, collection);

    return true;
  }

}