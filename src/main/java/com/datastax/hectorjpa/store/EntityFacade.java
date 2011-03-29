package com.datastax.hectorjpa.store;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.openjpa.kernel.OpenJPAStateManager;
import org.apache.openjpa.meta.ClassMetaData;
import org.apache.openjpa.meta.FieldMetaData;
import org.apache.openjpa.util.OpenJPAId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.hectorjpa.index.IndexDefinition;
import com.datastax.hectorjpa.index.IndexDefinitions;
import com.datastax.hectorjpa.meta.ColumnField;
import com.datastax.hectorjpa.meta.DiscriminatorColumn;
import com.datastax.hectorjpa.meta.IndexOperation;
import com.datastax.hectorjpa.meta.MetaCache;
import com.datastax.hectorjpa.meta.ObjectTypeColumnStrategy;
import com.datastax.hectorjpa.meta.StaticColumn;
import com.datastax.hectorjpa.meta.ToOneColumn;
import com.datastax.hectorjpa.meta.collection.AbstractCollectionField;
import com.datastax.hectorjpa.meta.collection.OrderedCollectionField;
import com.datastax.hectorjpa.meta.collection.UnorderedCollectionField;

public class EntityFacade implements Serializable {
  private static final Logger log = LoggerFactory.getLogger(EntityFacade.class);

  private static final long serialVersionUID = 4777260639119126462L;

  private final String columnFamilyName;
  private final Class<?> clazz;
  private final Serializer<?> keySerializer;
  private final ObjectTypeColumnStrategy strategy;
  private final IndexOperation[] indexOps;

  /**
   * Fields indexed by id
   */
  private final Map<Integer, ColumnField<?>> columnFieldIds;
  private final Map<Integer, AbstractCollectionField<?>> collectionFieldIds;

  /**
   * Default constructor
   * 
   * @param classMetaData
   *          The class meta data
   * @param mappingUtils
   *          The mapping utils to use for byte mapping
   */
  public EntityFacade(ClassMetaData classMetaData) {

    CassandraClassMetaData cassMeta = (CassandraClassMetaData) classMetaData;

    this.columnFamilyName = MappingUtils.getColumnFamily(cassMeta);

    this.keySerializer = MappingUtils.getSerializerForPk(cassMeta);

    clazz = cassMeta.getDescribedType();

    columnFieldIds = new HashMap<Integer, ColumnField<?>>();
    collectionFieldIds = new HashMap<Integer, AbstractCollectionField<?>>();

 
    // indexMetaData = new HashSet<AbstractEntityIndex>();

    FieldMetaData[] fmds = cassMeta.getFields();
    ColumnField<?> field = null;

    // parse all columns, we only want to do this on the first inti
    for (int i = 0; i < fmds.length; i++) {

      // not in the bit set to use, isn't managed or saved, or is a
      // primary key,
      // ignore
      if (fmds[i].getManagement() == FieldMetaData.MANAGE_NONE
          || fmds[i].isPrimaryKey()) {
        continue;
      }

      if (fmds[i].getAssociationType() == FieldMetaData.ONE_TO_MANY
          || fmds[i].getAssociationType() == FieldMetaData.MANY_TO_MANY) {

        AbstractCollectionField<?> collection = null;

        if (fmds[i].getOrders().length > 0) {
          collection = new OrderedCollectionField(fmds[i]);
        } else {
          collection = new UnorderedCollectionField(fmds[i]);
        }

        // TODO if fmds[i].getAssociationType() > 0 .. we found an
        // attached
        // entity
        // and need to find it's entityFacade
        collectionFieldIds.put(collection.getFieldId(), collection);

        // indexMetaData.add(new ManyEntityIndex(fmds[i],
        // mappingUtils));

        continue;
      }

      if (fmds[i].getAssociationType() == FieldMetaData.MANY_TO_ONE
          || fmds[i].getAssociationType() == FieldMetaData.ONE_TO_ONE) {

        ToOneColumn<?> toOne = new ToOneColumn(fmds[i]);

        columnFieldIds.put(i, toOne);

        continue;
      }

      if (log.isDebugEnabled()) {
        log.debug(
            "field name {} typeCode {} associationType: {} declaredType: {} embeddedMetaData: {}",
            new Object[] { fmds[i].getName(), fmds[i].getTypeCode(),
                fmds[i].getAssociationType(),
                fmds[i].getDeclaredType().getName(),
                fmds[i].getElement().getDeclaredTypeMetaData() });
      }

      field = new ColumnField(fmds[i]);

      // TODO if fmds[i].getAssociationType() > 0 .. we found an attached
      // entity
      // and need to find it's entityFacade
      columnFieldIds.put(field.getFieldId(), field);

    }

    // TODO TN pick the strategy if theres a discriminator

    String discriminator = cassMeta.getDiscriminatorColumn();

    if (discriminator != null || cassMeta.isAbstract()) {
      strategy = new DiscriminatorColumn(discriminator);
    } else {
      strategy = new StaticColumn();
    }

    IndexDefinitions indexDefs = cassMeta.getIndexDefinitions();

    if (indexDefs != null) {
      this.indexOps = new IndexOperation[indexDefs.getDefinitions().size()];

      for (int i = 0; i < indexOps.length; i++) {
        indexOps[i] = new IndexOperation(cassMeta, indexDefs.getDefinitions()
            .get(i));
      }

    } else {
      this.indexOps = null;
    }

  }

  // public String getColumnFamilyName() {
  // return columnFamilyName;
  // }
  //
  // public Class<?> getClazz() {
  // return clazz;
  // }
  //
  // public Serializer<?> getKeySerializer() {
  // return keySerializer;
  // }

  /**
   * Delete the entity with the given statemanager. The given clock time is used
   * for the delete of indexes
   * 
   * @param stateManager
   * @param mutator
   * @param clock
   */
  public void delete(OpenJPAStateManager stateManager, Mutator mutator,
      long clock) {
    mutator.addDeletion(MappingUtils.getKeyBytes(stateManager.getObjectId()),
        columnFamilyName, null, StringSerializer.get());
  }

  /**
   * Load all columns for this class specified in the bit set
   * 
   * @param stateManager
   * @param fieldSet
   * @return true if the entity was present (I.E the marker column was found)
   *         otherwise false is returned.
   */
  public boolean loadColumns(OpenJPAStateManager stateManager, BitSet fieldSet,
      Keyspace keyspace, MetaCache metaCache) {

    List<String> fields = new ArrayList<String>();

    ColumnField<?> field = null;
    AbstractCollectionField<?> collectionField = null;
    Object entityId = stateManager.getObjectId();

    boolean read = false;

    // This entity has never been persisted, we can't possibly load it
    if (MappingUtils.getTargetObject(entityId) == null) {
      return false;
    }

    // load all collections as we encounter them since they're seperate row
    // reads and construct columns for sliceQuery in primary CF
    for (int i = fieldSet.nextSetBit(0); i >= 0; i = fieldSet.nextSetBit(i + 1)) {
      field = columnFieldIds.get(i);

      if (field == null) {

        collectionField = collectionFieldIds.get(i);

        // nothting to do
        if (collectionField == null) {
          continue;
        }

        int size = stateManager.getContext().getFetchConfiguration()
            .getFetchBatchSize();

        // now query and load this field
        SliceQuery<byte[], DynamicComposite, byte[]> query = collectionField
            .createQuery(entityId, keyspace, size);

        read |= collectionField.readField(stateManager, query.execute());

        continue;
      }

      fields.add(field.getName());
    }

    // now load all the columns in the CF.
    SliceQuery<byte[], String, byte[]> query = MappingUtils.buildSliceQuery(
        entityId, fields, columnFamilyName, keyspace);

    QueryResult<ColumnSlice<String, byte[]>> result = query.execute();

    // read the field
    for (int i = fieldSet.nextSetBit(0); i >= 0; i = fieldSet.nextSetBit(i + 1)) {
      field = columnFieldIds.get(i);

      if (field == null) {
        continue;
      }

      read |= field.readField(stateManager, result);
    }

    // only need to check > 0. If the entity wasn't tombstoned then we would
    // have loaded the static jpa marker column
    if (!read) {
      return exists(stateManager, keyspace, metaCache);
    }

    return true;

  }

  /**
   * Loads only the jpa marker column to check for cassandra existence
   * 
   * @param stateManager
   * @param fieldSet
   * @return true if the entity was present (I.E the marker column was found)
   *         otherwise false is returned.
   */
  public boolean exists(OpenJPAStateManager stateManager, Keyspace keyspace,
      MetaCache metaCache) {
    return getStoredEntityType(stateManager.getObjectId(), keyspace, metaCache) != null;
  }

  /**
   * Get the stored entity and it's object id if it exists in the datastore
   * 
   * @param stateManager
   * @param keyspace
   * @return
   */
  public Class<?> getStoredEntityType(Object oid, Keyspace keyspace,
      MetaCache metaCache) {

    Class<?> oidType = ((OpenJPAId) oid).getType();

    // This entity has never been persisted, we can't possibly load it
    if (oidType == null) {
      return null;
    }

    String descrim = strategy.getStoredType(oid, columnFamilyName, keyspace);

    if (descrim == null) {
      return null;
    }

    return strategy.getClass(descrim, oidType, metaCache);

  }

  /**
   * Add the columns from the bit set to the mutator with the given time
   * 
   * @param stateManager
   * @param fieldSet
   * @param m
   * @param clockTime
   *          The time to use for write and deletes
   */
  public void addColumns(OpenJPAStateManager stateManager, BitSet fieldSet,
      Mutator<byte[]> m, long clockTime) {

    byte[] keyBytes = MappingUtils.getKeyBytes(stateManager.getObjectId());

    for (int i = fieldSet.nextSetBit(0); i >= 0; i = fieldSet.nextSetBit(i + 1)) {
      ColumnField field = columnFieldIds.get(i);

      if (field == null) {

        AbstractCollectionField<?> collection = collectionFieldIds.get(i);

        // nothing to do
        if (collection == null) {
          continue;
        }

        // we have a collection, persist it
        collection.addField(stateManager, m, clockTime, keyBytes,
            this.columnFamilyName);

        continue;

      }

      field.addField(stateManager, m, clockTime, keyBytes,
          this.columnFamilyName);

    }

    // add our object type column strategy
    this.strategy.write(m, clockTime, keyBytes, columnFamilyName);

    /**
     * We have indexes, write them
     */
    if (indexOps != null) {
      for (IndexOperation op : indexOps) {
        op.writeIndex(stateManager, m, clockTime);
      }

    }

  }
  
  

  /**
   * @return the indexOps
   */
  public IndexOperation[] getIndexOps() {
    return indexOps;
  }

  @Override
  public String toString() {

    return String.format(
        "EntityFacade[class: %s, columnFamily: %s, columnNames: %s]",
        clazz.getName(), columnFamilyName,
        Arrays.toString(columnFieldIds.entrySet().toArray()));

  }

}
