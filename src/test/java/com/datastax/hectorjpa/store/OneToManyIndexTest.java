package com.datastax.hectorjpa.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import javax.persistence.EntityManager;

import junit.framework.Assert;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.junit.Test;

import com.datastax.hectorjpa.ManagedEntityTestBase;
import com.datastax.hectorjpa.bean.Customer;
import com.datastax.hectorjpa.bean.ParentChildEntity;
import com.datastax.hectorjpa.bean.Phone;
import com.datastax.hectorjpa.bean.Phone.PhoneType;
import com.datastax.hectorjpa.bean.Store;
import com.google.common.collect.Lists;

/**
 * Test many to many indexing through an object graph. While many-to-many in
 * theory, in practice this is actually 2 one-to-many bi-directional
 * relationships.
 * 
 * @author Todd Nine
 * 
 */
public class OneToManyIndexTest extends ManagedEntityTestBase {

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void basicCasePersistence() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Store store = new Store();
    store.setName("Manhattan");

    Customer james = new Customer();
    james.setEmail("james@test.com");
    james.setName("James");
    james.setPhoneNumber(new Phone("+641112223333", PhoneType.MOBILE));

    Customer luke = new Customer();
    luke.setEmail("luke@test.com");
    luke.setName("Luke");
    luke.setPhoneNumber(new Phone("+64111222333", PhoneType.HOME));

    store.addCustomer(james);
    store.addCustomer(luke);

    em.persist(store);
    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    Store returnedStore = em2.find(Store.class, store.getId());

    /**
     * Make sure the stores are equal and everything is in sorted order
     */
    assertEquals(store, returnedStore);

    assertEquals(james, returnedStore.getCustomers().get(0));
    
    //test embedded objects
    assertEquals(james.getPhoneNumber(), returnedStore.getCustomers().get(0).getPhoneNumber());

    assertEquals(luke, returnedStore.getCustomers().get(1));
    
    assertEquals(luke.getPhoneNumber(), returnedStore.getCustomers().get(1).getPhoneNumber());

  }
  

  /**
   * It appears that changetrackers do not work with only a single element in the set.  In this case we have to detect set size of 0 and remove the reference in the collections.
   * without indexing
   */
  @Test
  public void lastElementRemoved() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Store store = new Store();
    store.setName("Manhattan");

    Customer james = new Customer();
    james.setEmail("james@test.com");
    james.setName("James");
    james.setPhoneNumber(new Phone("+641112223333", PhoneType.MOBILE));


    store.addCustomer(james);

    em.persist(store);
    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    Store returnedStore = em2.find(Store.class, store.getId());

    /**
     * Make sure the stores are equal and everything is in sorted order
     */
    assertEquals(store, returnedStore);

    assertEquals(james, returnedStore.getCustomers().get(0));
    
    //test embedded objects
    assertEquals(james.getPhoneNumber(), returnedStore.getCustomers().get(0).getPhoneNumber());


    em2.getTransaction().begin();
    
    returnedStore.getCustomers().remove(0);
    
    em2.getTransaction().commit();
    
    EntityManager em3 = entityManagerFactory.createEntityManager();
    
    returnedStore = em3.find(Store.class, store.getId());
    
    List<Customer> customers = returnedStore.getCustomers();
    
    
    assertEquals(0, customers.size());

  }
 
  

  /**
   * Test simple instance with no collections to ensure we persist properly
   * without indexing
   */
  @Test
  public void basicEmbeddedNullValue() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Store store = new Store();
    store.setName("Manhattan");

    Customer james = new Customer();
    james.setEmail("james@test.com");
    james.setName("James");
    james.setPhoneNumber(null);
    
    
    store.addCustomer(james);
    
    em.persist(store);
    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    
    Store returnedStore = em2.find(Store.class, store.getId());

    /**
     * Make sure the stores are equal and everything is in sorted order
     */
    assertEquals(store, returnedStore);

    assertEquals(james, returnedStore.getCustomers().get(0));
    
    Customer returnedCust = returnedStore.getCustomers().get(0);
    
    //test embedded objects
    assertNull(returnedCust.getPhoneNumber());
    

  }

  /**
   * Delete the first index and make sure it's removed
   */
  @Test
  public void basicDelete() {

    EntityManager em = entityManagerFactory.createEntityManager();
    em.getTransaction().begin();

    Store store = new Store();
    store.setName("Manhattan");

    Customer james = new Customer();
    james.setEmail("james@test.com");
    james.setName("James");
    james.setPhoneNumber(new Phone("+641112223333", PhoneType.MOBILE));

    Customer luke = new Customer();
    luke.setEmail("luke@test.com");
    luke.setName("Luke");
    luke.setPhoneNumber(new Phone("+641112223333", PhoneType.MOBILE));

    store.addCustomer(james);
    store.addCustomer(luke);

    em.persist(store);
    em.getTransaction().commit();
    em.close();

    EntityManager em2 = entityManagerFactory.createEntityManager();

    //now delete james and make sure it's not returned.
    em2.getTransaction().begin();
    
    Store returnedStore = em2.find(Store.class, store.getId());

    /**
     * Make sure the stores are equal and everything is in sorted order
     */
    assertEquals(store, returnedStore);

    assertEquals(james, returnedStore.getCustomers().get(0));

    assertEquals(luke, returnedStore.getCustomers().get(1));
    
    //remove james and save.  Should cause the element to be removed from the collection
    returnedStore.getCustomers().remove(0);
    
//    em2.persist(returnedStore);
    em2.getTransaction().commit();
    
    //now create a new em and pull the value out
    EntityManager em3 = entityManagerFactory.createEntityManager();
    
    returnedStore = em3.find(Store.class, store.getId());
    
    assertEquals(store, returnedStore);

    assertEquals(1, returnedStore.getCustomers().size());
    
    assertEquals(luke, returnedStore.getCustomers().get(0));
    

  }
  
  @Test
  public void recoverFromManualDelete() {
    final long parentId;
    final long childEntity2Id;
    
    // 1. Setup a a two child entity
    {
      final EntityManager em = entityManagerFactory.createEntityManager();
      
      em.getTransaction().begin();

      final ParentChildEntity parent = new ParentChildEntity();
      final ParentChildEntity child1 = new ParentChildEntity();
      final ParentChildEntity child2 = new ParentChildEntity();

      parent.setChildren(Lists.newArrayList(child1, child2));

      em.persist(parent);
      em.getTransaction().commit();
      em.close();
      childEntity2Id = child2.getId();
      parentId = parent.getId();
    }

    // 2. Do something stupid, in this case, fake a manual delete from a user in the db
    {
      final Cluster cluster = HFactory.getCluster("TestPool");
      final Keyspace keyspace = HFactory.createKeyspace("TestKeyspace", cluster);
      final Mutator<Long> mutator = HFactory.createMutator(keyspace, LongSerializer.get());

      mutator.addDeletion(childEntity2Id, "ParentChildEntity");
      mutator.execute();
    }

    // 3. Read it back, see that there is something missing, add another child
    {
      final EntityManager em = entityManagerFactory.createEntityManager();

      em.getTransaction().begin();

      final ParentChildEntity parent = em.find(ParentChildEntity.class, parentId);

      Assert.assertNotNull(parent);
      Assert.assertNotNull(parent.getChildren());
      Assert.assertEquals(1, parent.getChildren().size());

      // OK, so we only have 1, let's add back another child
      final List<ParentChildEntity> children = parent.getChildren();
      final ParentChildEntity child2 = new ParentChildEntity();

      children.add(child2);
      
      em.getTransaction().commit();
      // Make sure child 2 made it into the database
      Assert.assertEquals(2, parent.getChildren().size());
      Assert.assertNotNull(child2.getId());
      em.close();
    }
    
    // 4. Read it back, make sure we really have two children
    {
      final EntityManager em = entityManagerFactory.createEntityManager();

      em.getTransaction().begin();

      final ParentChildEntity parent = em.find(ParentChildEntity.class, parentId);
      
      Assert.assertNotNull(parent);
      Assert.assertNotNull(parent.getChildren());
      Assert.assertEquals(2, parent.getChildren().size());
    }
  }
}
