package com.datastax.hectorjpa.meta;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.openjpa.meta.ClassMetaData;

import com.datastax.hectorjpa.serialize.EmbeddedSerializer;
import com.datastax.hectorjpa.store.CassandraClassMetaData;
import com.datastax.hectorjpa.store.EntityFacade;

/**
 * Cache for holding all meta data
 * 
 * @author Todd Nine
 * 
 */
public class MetaCache {

  private final Map<ClassMetaData, EntityFacade> metaData = new HashMap<ClassMetaData, EntityFacade>();

  private final Map<String, CassandraClassMetaData> discriminators = new HashMap<String, CassandraClassMetaData>();

  /**
   * Required to ensure that more than 1 thread does not update our in memory meta cache at the same time.  Can cause slower
   * initialization, but ultimately will have better performance by ensuring only 1 instance of the entity facade exists
   * for the life of the JPA plugin execution
   */
  private final Object writeMutex = new Object();

  /**
   * Create a new meta cache for classes
   * 
   */
  public MetaCache() {

  }

  /**
   * Get the entity facade for this class. If it does not exist it is created
   * and added to the cache. Will return null if the given class meta data
   * cannot be directly persisted. Generally only applies to @MappedSuperclass
   * classes
   * 
   * @param meta
   * @return
   */
  public EntityFacade getFacade(ClassMetaData meta,
      EmbeddedSerializer serializer) {

    CassandraClassMetaData cassMeta = (CassandraClassMetaData) meta;

    EntityFacade facade = metaData.get(cassMeta);

    if (facade != null) {
      return facade;
    }

    synchronized (writeMutex) {

      // could be second into the mutex, check again
      facade = metaData.get(cassMeta);

      if (facade != null) {
        return facade;
      }

      return constructMetaData(cassMeta, serializer, new HashSet<CassandraClassMetaData>());
    }

  }

  private EntityFacade constructMetaData(CassandraClassMetaData cassMeta,
      EmbeddedSerializer serializer, Set<CassandraClassMetaData> visited) {

    if (visited.contains(cassMeta)) {
      return null;
    }

    visited.add(cassMeta);

    EntityFacade facade = null;

    // if it's a mapped super class we ignore it, there's nothing we can do
    // from
    // an entity facade perspective
    if (!cassMeta.isMappedSuperClass()) {
      facade = new EntityFacade(cassMeta, serializer);

      metaData.put(cassMeta, facade);

      String discriminatorValue = cassMeta.getDiscriminatorColumn();

      if (discriminatorValue != null) {

        discriminators.put(discriminatorValue, cassMeta);
      }
    }

    // if we have super or subclasses, they could be loaded at any point via
    // query without a persist. As a consequence
    // we must eagerly load all children and parents.
    for (ClassMetaData subClass : cassMeta.getPCSubclassMetaDatas()) {
      constructMetaData((CassandraClassMetaData) subClass, serializer, visited);
    }

    // load the parent
    ClassMetaData parentMeta = cassMeta.getPCSuperclassMetaData();

    if (parentMeta != null) {
      constructMetaData((CassandraClassMetaData) parentMeta, serializer,
          visited);
    }

    return facade;

  }

  /**
   * Get the class name from the discriminator string. Null if one doesn't exist
   * 
   * @param discriminator
   * @return
   */
  public CassandraClassMetaData getClassFromDiscriminator(String discriminator) {
    return discriminators.get(discriminator);
  }

}
