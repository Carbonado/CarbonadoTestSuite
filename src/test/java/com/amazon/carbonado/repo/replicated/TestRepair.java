/*
 * Copyright 2006 Amazon Technologies, Inc. or its affiliates.
 * Amazon, Amazon.com and Carbonado are trademarks or registered trademarks
 * of Amazon Technologies, Inc. or its affiliates.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazon.carbonado.repo.replicated;

import java.lang.reflect.Method;
import java.util.Random;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.CorruptEncodingException;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.OptimisticLockException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.UniqueConstraintException;

import com.amazon.carbonado.capability.ResyncCapability;

import com.amazon.carbonado.layout.StoredLayout;
import com.amazon.carbonado.layout.StoredLayoutProperty;

import com.amazon.carbonado.repo.replicated.ReplicatedRepository;
import com.amazon.carbonado.repo.replicated.ReplicatedRepositoryBuilder;

import com.amazon.carbonado.repo.sleepycat.BDBRepositoryBuilder;

import com.amazon.carbonado.TestUtilities;
import com.amazon.carbonado.stored.StorableTestBasic;
import com.amazon.carbonado.stored.StorableTestMinimal;
import com.amazon.carbonado.stored.StorableVersioned;

import com.amazon.carbonado.layout.TestLayout;
import org.cojen.classfile.TypeDesc;

/**
 *
 *
 * @author Brian S O'Neill
 */
public class TestRepair extends TestCase {
    private static final String REPLICA_NAME = "rr-replica";
    private static final String MASTER_NAME = "rr-master";

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestRepair.class);
    }

    private Repository mReplica;
    private Repository mMaster;
    private Repository mReplicated;

    public TestRepair(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        RepositoryBuilder replica = TestUtilities.newTempRepositoryBuilder(REPLICA_NAME);
        RepositoryBuilder master = TestUtilities.newTempRepositoryBuilder(MASTER_NAME);

        ReplicatedRepositoryBuilder builder = new ReplicatedRepositoryBuilder();
        builder.setName("rr");
        builder.setReplicaRepositoryBuilder(replica);
        builder.setMasterRepositoryBuilder(master);

        ReplicatedRepository rr = (ReplicatedRepository) builder.build();

        mReplica = rr.getReplicaRepository();
        mMaster = rr.getMasterRepository();
        mReplicated = rr;
    }

    protected void tearDown() throws Exception {
        if (mReplicated != null) {
            mReplicated.close();
        }
        if (mReplica != null) {
            mReplica.close();
        }
        if (mMaster != null) {
            mMaster.close();
        }
        mReplica = null;
        mMaster = null;
        mReplicated = null;
    }

    /**
     * @return repository locations
     */
    private String[] reOpenPersistent(String[] locations) throws Exception {
        tearDown();

        String replicaLocation, masterLocation;

        if (locations != null) {
            replicaLocation = locations[0];
            masterLocation = locations[1];
        } else {
            replicaLocation = TestUtilities.makeTestDirectoryString(REPLICA_NAME);
            masterLocation = TestUtilities.makeTestDirectoryString(MASTER_NAME);
        }

        {
            BDBRepositoryBuilder replica = new BDBRepositoryBuilder();
            replica.setName(REPLICA_NAME);
            replica.setTransactionNoSync(true);
            replica.setEnvironmentHome(replicaLocation);

            BDBRepositoryBuilder master = new BDBRepositoryBuilder();
            master.setName(MASTER_NAME);
            master.setTransactionNoSync(true);
            master.setEnvironmentHome(masterLocation);

            ReplicatedRepositoryBuilder builder = new ReplicatedRepositoryBuilder();
            builder.setName("rr");
            builder.setReplicaRepositoryBuilder(replica);
            builder.setMasterRepositoryBuilder(master);

            ReplicatedRepository rr = (ReplicatedRepository) builder.build();

            mReplica = rr.getReplicaRepository();
            mMaster = rr.getMasterRepository();
            mReplicated = rr;
        }

        return new String[] {replicaLocation, masterLocation};
    }

    public void testMissingEntry() throws Exception {
        // Insert an entry into master.
        {
            Storage<StorableTestBasic> storage = mMaster.storageFor(StorableTestBasic.class);
            StorableTestBasic stb = storage.prepare();
            stb.setId(5);
            stb.setStringProp("hello");
            stb.setIntProp(1);
            stb.setLongProp(1L);
            stb.setDoubleProp(1.0);
            stb.insert();
        }

        // Verify not available from rr.
        {
            Storage<StorableTestBasic> storage = mReplicated.storageFor(StorableTestBasic.class);
            StorableTestBasic stb = storage.prepare();
            stb.setId(5);
            assertFalse(stb.tryLoad());
        }

        // Insert into rr.
        {
            Storage<StorableTestBasic> storage = mReplicated.storageFor(StorableTestBasic.class);
            StorableTestBasic stb = storage.prepare();
            stb.setId(5);
            stb.setStringProp("world");
            stb.setIntProp(1);
            stb.setLongProp(1L);
            stb.setDoubleProp(1.0);
            try {
                stb.insert();
                fail();
            } catch (UniqueConstraintException e) {
            }
        }

        // Wait a moment for repair thread to finish.
        Thread.sleep(1000);

        // Verify available from rr.
        {
            Storage<StorableTestBasic> storage = mReplicated.storageFor(StorableTestBasic.class);
            StorableTestBasic stb = storage.prepare();
            stb.setId(5);
            assertTrue(stb.tryLoad());
            assertEquals("hello", stb.getStringProp());
        }
    }

    public void testMissingVersionedEntry() throws Exception {
        // Insert an entry into master.
        {
            Storage<StorableVersioned> storage = mMaster.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.setValue("hello");
            sv.insert();
        }

        // Verify not available from rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            assertFalse(sv.tryLoad());
        }

        // Insert into rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.setValue("world");
            try {
                sv.insert();
                fail();
            } catch (UniqueConstraintException e) {
            }
        }

        // Wait a moment for repair thread to finish.
        Thread.sleep(1000);

        // Verify available from rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            assertTrue(sv.tryLoad());
            assertEquals("hello", sv.getValue());
            assertEquals(1, sv.getVersion());
        }
    }

    public void testStaleEntry() throws Exception {
        // Insert an entry into rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.setValue("hello");
            sv.insert();
        }

        // Update master entry.
        {
            Storage<StorableVersioned> storage = mMaster.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            sv.setValue("world");
            sv.update();
        }

        // Verify old version in replica.
        {
            Storage<StorableVersioned> storage = mReplica.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            assertEquals(1, sv.getVersion());
            assertEquals("hello", sv.getValue());
        }

        // Attempt to update rr entry.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            assertEquals(1, sv.getVersion());
            assertEquals("hello", sv.getValue());

            sv.setValue("ciao");
            try {
                sv.update();
                fail();
            } catch (OptimisticLockException e) {
            }
        }

        // Wait a moment for repair thread to finish.
        Thread.sleep(1000);

        // Verify new version in rr and update it.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            assertEquals(2, sv.getVersion());
            assertEquals("world", sv.getValue());

            sv.setValue("ciao");
            sv.update();
        }
    }

    public void testStaleEntryAndBackoff() throws Exception {
        // Insert an entry into rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.setValue("hello");
            sv.insert();
        }

        // Update master entry.
        {
            Storage<StorableVersioned> storage = mMaster.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            sv.setValue("world");
            sv.update();
        }

        // Attempt to update rr entry.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);

            int failCount = 0;

            for (int retryCount = 3;;) {
                try {
                    sv.load();
                    sv.setValue("ciao");
                    sv.update();
                    break;
                } catch (OptimisticLockException e) {
                    failCount++;
                    retryCount = e.backoff(e, retryCount, 1000);
                }
            }

            assertTrue(failCount > 0);
        }

        // Verify new version in rr.
        {
            Storage<StorableVersioned> storage = mReplicated.storageFor(StorableVersioned.class);
            StorableVersioned sv = storage.prepare();
            sv.setID(5);
            sv.load();
            assertEquals(3, sv.getVersion());
            assertEquals("ciao", sv.getValue());
        }
    }

    public void testCorruptEntry() throws Exception {
        testCorruptEntry(false);
    }

    public void testCorruptEntryPreventDelete() throws Exception {
        testCorruptEntry(true);
    }

    private void testCorruptEntry(boolean preventDelete) throws Exception {
        // Close and open repository again, this time on disk. We need to close
        // and re-open the repository as part of the test.
        String[] locations = reOpenPersistent(null);

        // Insert different versions of the same record...

        final String recordName = "test.TheTestRecord";

        Class<? extends StorableTestMinimal> type0 = 
            TestLayout.defineStorable(recordName, 1, TypeDesc.INT);

        Class<? extends StorableTestMinimal> type1 = 
            TestLayout.defineStorable(recordName, 2, TypeDesc.INT);

        Storage<? extends StorableTestMinimal> storage0 = mReplicated.storageFor(type0);
        Storage<? extends StorableTestMinimal> storage1 = mReplicated.storageFor(type1);

        final int seed = 5469232;
        final int count = 20;

        {
            Random rnd = new Random(seed);

            Method prop_0_of_0 = type0.getMethod("setProp0", int.class);

            Method prop_0_of_1 = type1.getMethod("setProp0", int.class);
            Method prop_1_of_1 = type1.getMethod("setProp1", int.class);

            boolean anyType0 = false;
            boolean anyType1 = false;

            for (int i=0; i<count; i++) {
                StorableTestMinimal stm;

                if (rnd.nextBoolean()) {
                    stm = storage0.prepare();
                    prop_0_of_0.invoke(stm, i + 1000);
                    anyType0 = true;
                } else {
                    stm = storage1.prepare();
                    prop_0_of_1.invoke(stm, i + 2000);
                    prop_1_of_1.invoke(stm, i + 4000);
                    anyType1 = true;
                }

                stm.setId(i);
                stm.insert();
            }

            // Assert mix of types.
            assertTrue(anyType0);
            assertTrue(anyType1);
        }

        // Verify records can be read via storage0, which will ignore the new property.
        {
            Cursor<? extends StorableTestMinimal> cursor = storage0.query().fetch();
            while (cursor.hasNext()) {
                StorableTestMinimal stm = cursor.next();
                //System.out.println(stm);
            }
        }
        
        // Verify records can be read via storage1, which may have zero for the new property.
        {
            Cursor<? extends StorableTestMinimal> cursor = storage1.query().fetch();
            while (cursor.hasNext()) {
                StorableTestMinimal stm = cursor.next();
                //System.out.println(stm);
            }
        }

        // Close and open only replica repository and create corruption by
        // deleting all information regarding generation 1 in the replica.
        locations = reOpenPersistent(locations);

        storage0 = mReplicated.storageFor(type0);

        // Replace all the masters with only type 0 records.
        {
            mMaster.storageFor(type0).query().deleteAll();
            Method prop_0_of_0 = type0.getMethod("setProp0", int.class);

            for (int i=0; i<count; i++) {
                StorableTestMinimal stm = mMaster.storageFor(type0).prepare();
                prop_0_of_0.invoke(stm, i + 1000);
                stm.setId(i);
                stm.insert();
            }
        }

        // Delete all knowledge of type 1.
        StoredLayout layout = mReplicated.storageFor(StoredLayout.class).prepare();
        layout.setStorableTypeName(recordName);
        layout.setGeneration(1);
        layout.load();
        layout.delete();

        mReplicated.storageFor(StoredLayoutProperty.class)
            .query("layoutID = ?").with(layout.getLayoutID()).deleteAll();

        // Close and open to rebuild replicated repository.
        locations = reOpenPersistent(locations);

        storage0 = mReplicated.storageFor(type0);

        // Verify corruption. (Replica is unable to figure out what layout generation 1 is)
        try {
            Cursor<? extends StorableTestMinimal> cursor = storage0.query().fetch();
            while (cursor.hasNext()) {
                StorableTestMinimal stm = cursor.next();
            }
            fail();
        } catch (CorruptEncodingException e) {
        }

        // Resync to repair.

        Trigger trigger = null;
        if (preventDelete) {
            // The resync will try to delete the corrupt replica entries, but
            // this trigger will cause the delete to fail. Instead, the resync
            // will skip over these entries instead of crashing outright.
            trigger = new Trigger() {
                @Override
                public Object beforeDelete(Object s) throws PersistException {
                    throw new PersistException("Cannot delete me!");
                }
            };
            storage0.addTrigger(trigger);
        }

        ResyncCapability cap = mReplicated.getCapability(ResyncCapability.class);
        cap.resync(type0, 1.0, null);

        if (preventDelete) {
            // If this point is reached, the resync didn't crash, but it hasn't
            // actually repaired the broken records. Remove the trigger and do
            // the resync again.
            storage0.removeTrigger(trigger);
            cap.resync(type0, 1.0, null);
        }

        {
            // Verify records can be read out now.
            Cursor<? extends StorableTestMinimal> cursor = storage0.query().fetch();
            int actual = 0;
            while (cursor.hasNext()) {
                StorableTestMinimal stm = cursor.next();
                //System.out.println(stm);
                actual++;
            }
            assertEquals(count, actual);
        }

        storage1 = mReplicated.storageFor(type1);

        {
            Cursor<? extends StorableTestMinimal> cursor = storage1.query().fetch();
            int actual = 0;
            while (cursor.hasNext()) {
                StorableTestMinimal stm = cursor.next();
                //System.out.println(stm);
                actual++;
            }
            assertEquals(count, actual);
        }
    }
}
