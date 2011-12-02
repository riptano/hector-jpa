/**
 * 
 */
package com.datastax.hectorjpa.index;

import static com.datastax.hectorjpa.serializer.CompositeUtils.newComposite;

import java.nio.ByteBuffer;

import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.openjpa.kernel.OpenJPAStateManager;

import com.datastax.hectorjpa.query.IndexQuery;
import com.datastax.hectorjpa.query.field.FieldExpression;
import com.datastax.hectorjpa.query.iterator.ScanBuffer;
import com.datastax.hectorjpa.service.IndexAudit;
import com.datastax.hectorjpa.service.IndexQueue;
import com.datastax.hectorjpa.store.CassandraClassMetaData;

/**
 * Class to perform all operations for secondary indexing on an instance in the
 * statemanager.
 * 
 * @author Todd Nine
 * 
 */
public class SubclassIndexOperation extends AbstractIndexOperation {
  // /**
  // * String array of all subclass discriminator values
  // */
  // private String[] subClasses;
  //
  //
  // //the discirminator value for the class that owns this instance. I.E. same
  // from class metaData
  // private String discriminatorValue;

  private ByteBuffer searchRowKey;

  private ByteBuffer[] parentSearchRowKeys;

  private ByteBuffer tombstoneRowKey;

  private ByteBuffer[] parentTombstoneRowKeys;

  public SubclassIndexOperation(CassandraClassMetaData metaData,
      IndexDefinition indexDef) {
    super(metaData, indexDef);

    String[] subClasses = metaData.getSuperClassDiscriminators();

    String discriminatorValue = metaData.getDiscriminatorColumn();

    searchRowKey = createIndexKey(discriminatorValue, searchIndexNameString);
    tombstoneRowKey = createIndexKey(discriminatorValue, reverseIndexNameString);

    parentSearchRowKeys = new ByteBuffer[subClasses.length];
    parentTombstoneRowKeys = new ByteBuffer[subClasses.length];

    for (int i = 0; i < subClasses.length; i++) {
      parentSearchRowKeys[i] = createIndexKey(subClasses[i],
          searchIndexNameString);
      parentTombstoneRowKeys[i] = createIndexKey(subClasses[i],
          reverseIndexNameString);
    }

  }

  /**
   * Write the index definition
   * 
   * @param stateManager
   *          The objects state manager
   * @param mutator
   *          The mutator to write to
   * @param clock
   *          the clock value to use
   */
  public void writeIndex(OpenJPAStateManager stateManager,
      Mutator<ByteBuffer> mutator, long clock, IndexQueue queue) {

    DynamicComposite searchComposite = null;
    DynamicComposite tombstoneComposite = null;
    DynamicComposite idAudit = null;

    // loop through all added objects and create the writes for them.
    // create our composite of the format of id+order*

    for (int i = 0; i < parentSearchRowKeys.length; i++) {

      searchComposite = newComposite();

      // create our composite of the format order*+id

      tombstoneComposite = newComposite();

      idAudit = newComposite();

      constructComposites(searchComposite, tombstoneComposite, idAudit,
          stateManager);

      mutator.addInsertion(parentSearchRowKeys[i], CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(searchComposite, HOLDER,
              clock, compositeSerializer, bytesSerializer));

      mutator.addInsertion(parentTombstoneRowKeys[i], CF_NAME,
          new HColumnImpl<DynamicComposite, byte[]>(tombstoneComposite, HOLDER,
              clock, compositeSerializer, bytesSerializer));

      queue.addAudit(new IndexAudit(parentSearchRowKeys[i],
          parentTombstoneRowKeys[i], idAudit, clock, CF_NAME, true));

    }

    // write the root level entries
    searchComposite = newComposite();

    // create our composite of the format order*+id

    tombstoneComposite = newComposite();

    idAudit = newComposite();

    constructComposites(searchComposite, tombstoneComposite, idAudit,
        stateManager);

    mutator.addInsertion(searchRowKey, CF_NAME,
        new HColumnImpl<DynamicComposite, byte[]>(searchComposite, HOLDER,
            clock, compositeSerializer, bytesSerializer));

    mutator.addInsertion(tombstoneRowKey, CF_NAME,
        new HColumnImpl<DynamicComposite, byte[]>(tombstoneComposite, HOLDER,
            clock, compositeSerializer, bytesSerializer));

    queue.addAudit(new IndexAudit(searchRowKey, tombstoneRowKey, idAudit,
        clock, CF_NAME, true));

  }

  @Override
  protected ByteBuffer getScanKey() {
    return searchRowKey;
  }

  @Override
  protected void queueDeletes(DynamicComposite searchCol, IndexQueue queue,
      long clock) {

    queue.addDelete(new IndexAudit(searchRowKey, tombstoneRowKey, searchCol,
        clock, CF_NAME, true));

    for (int i = 0; i < parentSearchRowKeys.length; i++) {
      queue.addDelete(new IndexAudit(parentSearchRowKeys[i],
          parentTombstoneRowKeys[i], searchCol, clock, CF_NAME, true));
    }

  }

}
