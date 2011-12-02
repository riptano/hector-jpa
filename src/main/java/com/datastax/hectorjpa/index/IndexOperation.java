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
 * statemanager
 * 
 * @author Todd Nine
 * 
 */
public class IndexOperation extends AbstractIndexOperation {

  private ByteBuffer forwardIndexKey;
  private ByteBuffer reverseIndexKey;

  public IndexOperation(CassandraClassMetaData metaData,
      IndexDefinition indexDef) {
    super(metaData, indexDef);

    String className = metaData.getDescribedTypeString();

    forwardIndexKey = createIndexKey(className, searchIndexNameString);
    reverseIndexKey = createIndexKey(className, reverseIndexNameString);

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

    DynamicComposite searchComposite = newComposite();
    DynamicComposite tombstoneComposite = newComposite();
    DynamicComposite idAudit = newComposite();

    // create our composite of the format order*+id

    // create our composite of the format id+order*

    constructComposites(searchComposite, tombstoneComposite, idAudit,
        stateManager);

    mutator.addInsertion(forwardIndexKey, CF_NAME,
        new HColumnImpl<DynamicComposite, byte[]>(searchComposite, HOLDER,
            clock, compositeSerializer, bytesSerializer));

    mutator.addInsertion(reverseIndexKey, CF_NAME,
        new HColumnImpl<DynamicComposite, byte[]>(tombstoneComposite, HOLDER,
            clock, compositeSerializer, bytesSerializer));

    queue.addAudit(new IndexAudit(forwardIndexKey, reverseIndexKey, idAudit,
        clock, CF_NAME, true));

  }


  @Override
  protected ByteBuffer getScanKey() {
    return forwardIndexKey;
  }

  @Override
  protected void queueDeletes(DynamicComposite searchCol, IndexQueue queue, long clock) {
    queue.addDelete(new IndexAudit(forwardIndexKey, reverseIndexKey, searchCol,
        clock, CF_NAME, true));
    
  }

}
