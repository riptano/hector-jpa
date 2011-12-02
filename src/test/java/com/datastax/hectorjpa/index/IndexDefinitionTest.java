package com.datastax.hectorjpa.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class IndexDefinitionTest {

  /**
   * Tests when 2 indexes use the same fields defined in a different order, both
   * hash code and equals are the same since they are ultimately the same fields
   */
  @Test
  public void equalsDiffFieldOrdering() {

    IndexField[] first = new IndexField[] { new IndexField("field1"),
        new IndexField("field2"), new IndexField("field3") };

    IndexDefinition firstDef = new IndexDefinition(first, new IndexOrder[] {});

    IndexField[] second = new IndexField[] { new IndexField("field3"),
        new IndexField("field1"), new IndexField("field2") };

    IndexDefinition secondDef = new IndexDefinition(second, new IndexOrder[] {});

    assertEquals(firstDef.hashCode(), secondDef.hashCode());

    assertEquals(firstDef, secondDef);
  }

  /**
   * Tests when 2 indexes use the same fields and no order, both hash code and
   * equals are the same
   */
  @Test
  public void equalsNoOrder() {

    IndexField[] first = new IndexField[] { new IndexField("field1"),
        new IndexField("field2"), new IndexField("field3") };

    IndexDefinition firstDef = new IndexDefinition(first, new IndexOrder[] {});

    IndexField[] second = new IndexField[] { new IndexField("field1"),
        new IndexField("field2"), new IndexField("field3") };

    IndexDefinition secondDef = new IndexDefinition(second, new IndexOrder[] {});

    assertEquals(firstDef.hashCode(), secondDef.hashCode());

    assertEquals(firstDef, secondDef);
  }

  /**
   * Tests that when fields are the same and order is used they match
   */
  @Test
  public void equalsOrder() {
    IndexField[] first = new IndexField[] { new IndexField("field1"),
        new IndexField("field2"), new IndexField("field3") };

    IndexOrder[] firstOrder = new IndexOrder[] { new IndexOrder("field4", true) };

    IndexDefinition firstDef = new IndexDefinition(first, firstOrder);

    IndexField[] second = new IndexField[] { new IndexField("field1"),
        new IndexField("field2"), new IndexField("field3") };

    IndexOrder[] secondOrder = new IndexOrder[] { new IndexOrder("field4", true) };

    IndexDefinition secondDef = new IndexDefinition(second, secondOrder);

    assertEquals(firstDef.hashCode(), secondDef.hashCode());

    assertEquals(firstDef, secondDef);
  }

  /**
   * Tests that when ordering of fields is different, they Index defs are not
   * equals
   */
  @Test
  public void equalsDiffOrder() {

    IndexField[] first = new IndexField[] { new IndexField("field1"),
        new IndexField("field2"), new IndexField("field3") };

    IndexOrder[] firstOrder = new IndexOrder[] { new IndexOrder("field4", true) };

    IndexDefinition firstDef = new IndexDefinition(first, firstOrder);

    IndexField[] second = new IndexField[] { new IndexField("field1"),
        new IndexField("field2"), new IndexField("field3") };

    IndexOrder[] secondOrder = new IndexOrder[] { new IndexOrder("field4",
        false) };

    IndexDefinition secondDef = new IndexDefinition(second, secondOrder);

    assertTrue(firstDef.hashCode() != secondDef.hashCode());

    assertFalse(firstDef.equals(secondDef));
  }
  
  /**
   * Tests that when ordering of fields is different, they Index defs are not
   * equals
   */
  @Test
  public void equalsDiffOrderField() {

    IndexField[] first = new IndexField[] { new IndexField("field1"),
        new IndexField("field2"), new IndexField("field3") };

    IndexOrder[] firstOrder = new IndexOrder[] { new IndexOrder("field4", true),  new IndexOrder("field5", true) };

    IndexDefinition firstDef = new IndexDefinition(first, firstOrder);

    IndexField[] second = new IndexField[] { new IndexField("field1"),
        new IndexField("field2"), new IndexField("field3") };

    IndexOrder[] secondOrder = new IndexOrder[] { new IndexOrder("field5", true),  new IndexOrder("field4", true) };

    IndexDefinition secondDef = new IndexDefinition(second, secondOrder);

    assertTrue(firstDef.hashCode() != secondDef.hashCode());

    assertFalse(firstDef.equals(secondDef));
  }

  /**
   * Tests that when orders are the same and fields are different, they are not
   * equals
   */
  @Test
  public void equalsDiffFields() {

    IndexField[] first = new IndexField[] { new IndexField("field1"),
        new IndexField("field2"), new IndexField("field3") };

    IndexDefinition firstDef = new IndexDefinition(first, new IndexOrder[]{});

    IndexField[] second = new IndexField[] { new IndexField("field1"),
        new IndexField("field2"), new IndexField("field4") };

    IndexDefinition secondDef = new IndexDefinition(second, new IndexOrder[]{});

    assertTrue(firstDef.hashCode() != secondDef.hashCode());

    assertFalse(firstDef.equals(secondDef));
  }
  
  /**
   * Tests that when orders are the same and fields are different, they are not
   * equals
   */
  @Test
  public void orderedIssue() {
    
    IndexField[] first = new IndexField[] { new IndexField("userId"),
        new IndexField("startSaved"), new IndexField("endSaved") };

    IndexDefinition firstDef = new IndexDefinition(first, new IndexOrder[]{});

    IndexField[] second = new IndexField[] { new IndexField("startSaved"),
        new IndexField("userId"), new IndexField("endSaved") };

    IndexDefinition secondDef = new IndexDefinition(second, new IndexOrder[]{});

    assertEquals(firstDef.hashCode(), secondDef.hashCode());

    assertEquals(firstDef, secondDef);
  }



}
