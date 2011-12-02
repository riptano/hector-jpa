/**
 * 
 */
package com.datastax.hectorjpa.query.iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;
import java.util.List;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.IntegerSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

import com.datastax.hectorjpa.CassandraTestBase;
import com.datastax.hectorjpa.index.AbstractIndexOperation;
import com.datastax.hectorjpa.serializer.TimeUUIDSerializer;
import com.eaio.uuid.UUID;

/**
 * Tests all cases for single scan integration
 * 
 * @author Todd Nine
 * 
 */
public class ScanBufferTest extends CassandraTestBase {

  private static final ByteBuffer holder = ByteBuffer.wrap(new byte[]{ 0 });

  /**
   * Tests the case where start is 0 and the size is within the fetch size
   * window
   */
  @Test
  public void preLoaded() {

    ByteBuffer rowKey = generateComposites(100);

    ScanBuffer iterator = new ScanBuffer(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(100,
            ComponentEquality.GREATER_THAN_EQUAL), rowKey);

    int advanced = iterator.advance(0);

    assertEquals(0, advanced);

    assertNull(iterator.current());

    int loaded = iterator.loadNext(100);

    assertEquals(100, loaded);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(0, value);

  }

  /**
   * Tests the case where start is 0 and the loaded count is greater than
   * cassandra rows window
   */
  @Test
  public void noRecordsLeft() {

    ByteBuffer rowKey = generateComposites(100);

    ScanBuffer iterator = new ScanBuffer(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(100,
            ComponentEquality.GREATER_THAN_EQUAL), rowKey);

    int advanced = iterator.advance(0);

    assertEquals(0, advanced);

    assertNull(iterator.current());

    int loaded = iterator.loadNext(101);

    assertEquals(100, loaded);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(0, value);

  }

  /**
   * Tests the case where start is in the middle of the loaded range and the
   * loaded count is greater than cassandra rows window
   */
  @Test
  public void preloadedAdvance() {

    ByteBuffer rowKey = generateComposites(100);

    ScanBuffer iterator = new ScanBuffer(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(100,
            ComponentEquality.GREATER_THAN_EQUAL), rowKey);

    int advanced = iterator.advance(50);

    assertEquals(50, advanced);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(49, value);

    int loaded = iterator.loadNext(50);

    assertEquals(50, loaded);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(49, value);

    iterator.advance(50);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(99, value);

  }

  /**
   * Tests the case where start is 0 and the loaded count is greater than
   * cassandra rows window
   */
  @Test
  public void preloadedNoRecordsLeft() {

    ByteBuffer rowKey = generateComposites(100);

    ScanBuffer iterator = new ScanBuffer(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(100,
            ComponentEquality.GREATER_THAN_EQUAL), rowKey);

    // advance 0
    int advanced = iterator.advance(50);

    assertEquals(50, advanced);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(49, value);

    int loaded = iterator.loadNext(51);

    assertEquals(50, loaded);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(49, value);

    iterator.advance(50);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(99, value);

  }

  /**
   * Tests the case where start is 0 and the loaded count is greater than
   * cassandra rows window
   */
  @Test
  public void multiPageAdvance() {

    ByteBuffer rowKey = generateComposites(2000);

    ScanBuffer iterator = new ScanBuffer(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), null, rowKey);

    // advance 0
    int advanced = iterator.advance(1400);

    assertEquals(1400, advanced);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(1399, value);

    // there should only be 200 that are loaded
    int loaded = iterator.loadNext(1000);

    assertEquals(600, loaded);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(1399, value);

  }

  /**
   * Tests the case where start is 0 and the loaded count is greater than
   * cassandra rows window
   */
  @Test
  public void multiPageAdvanceThenLoad() {

    ByteBuffer rowKey = generateComposites(2000);

    ScanBuffer iterator = new ScanBuffer(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), null, rowKey);

    // advance 0
    int advanced = iterator.advance(1400);

    assertEquals(1400, advanced);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(1399, value);

    // there should only be 200 that are loaded
    int loaded = iterator.loadNext(1000);

    assertEquals(600, loaded);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(1399, value);

    iterator.advance(600);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(1999, value);

  }

  /**
   * Tests the case where we advance to the very end of the set. From there
   * loadNext will return no values
   */
  @Test
  public void multiPageAdvanceUpperLimit() {

    ByteBuffer rowKey = generateComposites(1000);

    ScanBuffer iterator = new ScanBuffer(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(799,
            ComponentEquality.EQUAL), rowKey);

    // advance 800
    int advanced = iterator.advance(800);

    assertEquals(800, advanced);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(799, value);

    // there should only be 200 that are loaded from the saved range, but the
    // upper limit of the range scan restricts the value
    // therefore 0 should be loaded
    int loaded = iterator.loadNext(300);

    assertEquals(0, loaded);

    value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(799, value);

  }

  /**
   * Tests the case where we advance to the very end of the set. From there
   * loadNext will return no values
   */
  @Test
  public void loadWithoutAdvance() {

    ByteBuffer rowKey = generateComposites(1000);

    ScanBuffer iterator = new ScanBuffer(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), genComposite(799,
            ComponentEquality.EQUAL), rowKey);

    // there should only be 200 that are loaded
    int loaded = iterator.loadNext(1000);

    assertEquals(800, loaded);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(0, value);

  }

  /**
   * Tests the case where we advance to the very end of the set. From there
   * loadNext will return no values
   */
  @Test
  public void loadWithoutAdvanceBeyondRange() {

    ByteBuffer rowKey = generateComposites(1000);

    ScanBuffer iterator = new ScanBuffer(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), null, rowKey);

    // there should only be 200 that are loaded
    int loaded = iterator.loadNext(2000);

    assertEquals(1000, loaded);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(0, value);

    // can only advance 999 b/c we're already at index 0
    int advanced = iterator.advance(1000);

    assertEquals(999, advanced);

    assertNull(iterator.current());
  }

  /**
   * Tests the case where start is 0 and the loaded count is greater than
   * cassandra rows window
   */
  @Test
  public void multiPageLoad() {

    ByteBuffer rowKey = generateComposites(1000);

    ScanBuffer iterator = new ScanBuffer(CassandraTestBase.keyspace,
        genComposite(0, ComponentEquality.EQUAL), null, rowKey);

    // advance 0
    int advanced = iterator.advance(0);

    assertEquals(0, advanced);

    assertNull(iterator.current());

    // there should only be 200 that are loaded
    int loaded = iterator.loadNext(2000);

    assertEquals(1000, loaded);

    int value = iterator.current().get(0, IntegerSerializer.get());

    assertEquals(0, value);

    // advance beyond the possible range
    iterator.advance(2000);

    assertNull(iterator.current());

  }

  /**
   * Test ordering composites
   * 
   * @param size
   */
  @Test
  public void testCompositeOrdering() {

    Mutator<ByteBuffer> mutator = HFactory.createMutator(
        CassandraTestBase.keyspace, ByteBufferSerializer.get());

    ByteBuffer rowKey = generateRowKey();

    System.out.println("Insertion values");

    DynamicComposite composite = new DynamicComposite();

    // make all values the same first column
    composite.addComponent("jeans", StringSerializer.get(), StringSerializer
        .get().getComparatorType().getTypeName());

    // now increment the second value

    composite.addComponent(1293840000000l, LongSerializer.get(),
        "BytesType(reversed=true)");

    // composite.addComponent(0l, LongSerializer.get(),
    // LongSerializer.get().getComparatorType().getTypeName());
    // composite.addComponent(i, LongSerializer.get(),
    // LongSerializer.get().getComparatorType().getTypeName());

    mutator.addInsertion(
        rowKey,
        AbstractIndexOperation.CF_NAME,
        HFactory.createColumn(composite, holder,
            DynamicCompositeSerializer.get(), ByteBufferSerializer.get()));

    System.out.println(ByteBufferUtil.bytesToHex(composite.serialize()));

    composite = new DynamicComposite();

    // make all values the same first column
    composite.addComponent("jeans", StringSerializer.get(), StringSerializer
        .get().getComparatorType().getTypeName());

    // now increment the second value

    composite.addComponent(1294099200000l, LongSerializer.get(),
        "BytesType(reversed=true)");

    // composite.addComponent(0l, LongSerializer.get(),
    // LongSerializer.get().getComparatorType().getTypeName());
    // composite.addComponent(i, LongSerializer.get(),
    // LongSerializer.get().getComparatorType().getTypeName());

    mutator.addInsertion(
        rowKey,
        AbstractIndexOperation.CF_NAME,
        HFactory.createColumn(composite, holder,
            DynamicCompositeSerializer.get(), ByteBufferSerializer.get()));

    System.out.println(ByteBufferUtil.bytesToHex(composite.serialize()));

    mutator.execute();

    // now query them with a scan and ensure they're returned correctly.

    SliceQuery<ByteBuffer, DynamicComposite, ByteBuffer> sliceQuery = HFactory
        .createSliceQuery(keyspace, ByteBufferSerializer.get(),
            DynamicCompositeSerializer.get(), ByteBufferSerializer.get());

    sliceQuery.setColumnFamily(AbstractIndexOperation.CF_NAME);
    sliceQuery.setKey(rowKey);

    DynamicComposite start = new DynamicComposite();
    start.addComponent("jeans", StringSerializer.get(), StringSerializer.get()
        .getComparatorType().getTypeName(), ComponentEquality.EQUAL);

    DynamicComposite end = new DynamicComposite();
    end.addComponent("jeans", StringSerializer.get(), StringSerializer.get()
        .getComparatorType().getTypeName(),
        ComponentEquality.GREATER_THAN_EQUAL);

    sliceQuery.setRange(start, end, false, 1000);

    System.out.println("Range values");
    System.out.println(ByteBufferUtil.bytesToHex(start.serialize()));
    System.out.println(ByteBufferUtil.bytesToHex(end.serialize()));

    List<HColumn<DynamicComposite, ByteBuffer>> cols = sliceQuery.execute()
        .get().getColumns();

    System.out.println("Returned values");

    System.out.println(ByteBufferUtil.bytesToHex(cols.get(0).getNameBytes()));

    composite = cols.get(0).getName();

    // make all values the same first column

    assertEquals("jeans", composite.get(0, StringSerializer.get()));

    assertEquals(1294099200000l, (long) composite.get(1, LongSerializer.get()));

    composite = cols.get(1).getName();

    // make all values the same first column

    assertEquals("jeans", composite.get(0, StringSerializer.get()));

    assertEquals(1293840000000l, (long) composite.get(1, LongSerializer.get()));

  }
  

  /**
   * Test ordering composites
   * 
   * @param size
   */
  @Test
  public void testCompositeOrderingPass() {

    Mutator<ByteBuffer> mutator = HFactory.createMutator(
        CassandraTestBase.keyspace, ByteBufferSerializer.get());

    ByteBuffer rowKey = generateRowKey();

    System.out.println("Insertion values");

    DynamicComposite composite = new DynamicComposite();

    // make all values the same first column
    composite.addComponent("jeans", StringSerializer.get(), StringSerializer
        .get().getComparatorType().getTypeName());

    // now increment the second value

    composite.addComponent(0l, LongSerializer.get(),
        "BytesType(reversed=true)");

    // composite.addComponent(0l, LongSerializer.get(),
    // LongSerializer.get().getComparatorType().getTypeName());
    // composite.addComponent(i, LongSerializer.get(),
    // LongSerializer.get().getComparatorType().getTypeName());

    mutator.addInsertion(
        rowKey,
        AbstractIndexOperation.CF_NAME,
        HFactory.createColumn(composite, holder,
            DynamicCompositeSerializer.get(), ByteBufferSerializer.get()));

    System.out.println(ByteBufferUtil.bytesToHex(composite.serialize()));

    composite = new DynamicComposite();

    // make all values the same first column
    composite.addComponent("jeans", StringSerializer.get(), StringSerializer
        .get().getComparatorType().getTypeName());

    // now increment the second value

    composite.addComponent(1l, LongSerializer.get(),
        "BytesType(reversed=true)");

    // composite.addComponent(0l, LongSerializer.get(),
    // LongSerializer.get().getComparatorType().getTypeName());
    // composite.addComponent(i, LongSerializer.get(),
    // LongSerializer.get().getComparatorType().getTypeName());

    mutator.addInsertion(
        rowKey,
        AbstractIndexOperation.CF_NAME,
        HFactory.createColumn(composite, holder,
            DynamicCompositeSerializer.get(), ByteBufferSerializer.get()));

    System.out.println(ByteBufferUtil.bytesToHex(composite.serialize()));

    mutator.execute();

    // now query them with a scan and ensure they're returned correctly.

    SliceQuery<ByteBuffer, DynamicComposite, ByteBuffer> sliceQuery = HFactory
        .createSliceQuery(keyspace, ByteBufferSerializer.get(),
            DynamicCompositeSerializer.get(), ByteBufferSerializer.get());

    sliceQuery.setColumnFamily(AbstractIndexOperation.CF_NAME);
    sliceQuery.setKey(rowKey);

    DynamicComposite start = new DynamicComposite();
    start.addComponent("jeans", StringSerializer.get(), StringSerializer.get()
        .getComparatorType().getTypeName(), ComponentEquality.EQUAL);
    
    DynamicComposite end = new DynamicComposite();
    end.addComponent("jeans", StringSerializer.get(), StringSerializer.get()
        .getComparatorType().getTypeName(),
        ComponentEquality.GREATER_THAN_EQUAL);
    

    sliceQuery.setRange(start, end, false, 1000);

    System.out.println("Range values");
    System.out.println(ByteBufferUtil.bytesToHex(start.serialize()));
    System.out.println(ByteBufferUtil.bytesToHex(end.serialize()));

    List<HColumn<DynamicComposite, ByteBuffer>> cols = sliceQuery.execute()
        .get().getColumns();

    System.out.println("Returned values");

    System.out.println(ByteBufferUtil.bytesToHex(cols.get(0).getNameBytes()));

    composite = cols.get(0).getName();

    // make all values the same first column

    assertEquals("jeans", composite.get(0, StringSerializer.get()));

    assertEquals(1l, (long) composite.get(1, LongSerializer.get()));

    composite = cols.get(1).getName();

    // make all values the same first column

    assertEquals("jeans", composite.get(0, StringSerializer.get()));

    assertEquals(0l, (long) composite.get(1, LongSerializer.get()));

  }

  private DynamicComposite genComposite(int start, ComponentEquality equality) {
    DynamicComposite composite = new DynamicComposite();
    composite.addComponent(start, IntegerSerializer.get(), IntegerSerializer
        .get().getComparatorType().getTypeName(), equality);

    return composite;
  }

  /**
   * Generate the given number of composites and returns the key for the row
   * 
   * @param size
   */
  private ByteBuffer generateComposites(int size) {

    Mutator<ByteBuffer> mutator = HFactory.createMutator(
        CassandraTestBase.keyspace, ByteBufferSerializer.get());
  
    ByteBuffer rowKey = generateRowKey();

    for (int i = 0; i < size; i++) {

      DynamicComposite composite = new DynamicComposite();
      composite.addComponent(i, IntegerSerializer.get(), IntegerSerializer
          .get().getComparatorType().getTypeName());

      mutator.addInsertion(
          rowKey,
          AbstractIndexOperation.CF_NAME,
          HFactory.createColumn(composite, holder,
              DynamicCompositeSerializer.get(), ByteBufferSerializer.get()));
    }

    mutator.execute();

    return rowKey;

  }

  /**
   * Generates a fake row key for testing
   * 
   * @return
   */
  private ByteBuffer generateRowKey() {
    return TimeUUIDSerializer.get().toByteBuffer(new UUID());
  }
}
