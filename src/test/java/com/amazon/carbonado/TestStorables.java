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

package com.amazon.carbonado;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.amazon.carbonado.ConstraintException;
import com.amazon.carbonado.Cursor;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.FetchNoneException;
import com.amazon.carbonado.OptimisticLockException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.PersistMultipleException;
import com.amazon.carbonado.PersistNoneException;
import com.amazon.carbonado.PrimaryKey;
import com.amazon.carbonado.Query;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.Trigger;
import com.amazon.carbonado.UniqueConstraintException;

import com.amazon.carbonado.cursor.SortedCursor;

import com.amazon.carbonado.lob.Blob;
import com.amazon.carbonado.lob.ByteArrayBlob;
import com.amazon.carbonado.lob.Clob;
import com.amazon.carbonado.lob.StringClob;

import com.amazon.carbonado.spi.RepairExecutor;

import com.amazon.carbonado.stored.*;

/**
 * Runs an extensive set of acceptance tests for a repository. Must be
 * subclassed to change the repository to use.
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 */
public class TestStorables extends TestCase {

    public static final long sSetId = 0x1L << 0;           // 0x0001
    public static final long sGetStringProp = 0x1L << 1;   // 0x0002
    public static final long sSetStringProp = 0x1L << 2;   // 0x0004
    public static final long sGetIntProp = 0x1L << 3;      // 0x0008
    public static final long sSetIntProp = 0x1L << 4;      // 0x0010
    public static final long sGetLongProp = 0x1L << 5;     // 0x0020
    public static final long sSetLongProp = 0x1L << 6;     // 0x0040
    public static final long sGetDoubleProp = 0x1L << 7;   // 0x0080
    public static final long sSetDoubleProp = 0x1L << 8;   // 0x0100
    public static final long sLoad = 0x1L << 9;            // 0x0200
    public static final long sTryLoad = 0x1L << 10;        // 0x0400
    public static final long sInsert = 0x1L << 11;         // 0x0800
    public static final long sTryInsert = 0x1L << 31;      // 0x8000 0000
    public static final long sUpdate = 0x1L << 32;         // 0x0001 0000 0000
    public static final long sTryUpdate = 0x1L << 12;      // 0x1000
    public static final long sDelete = 0x1L << 33;         // 0x0002 0000 0000
    public static final long sTryDelete = 0x1L << 13;      // 0x2000
    public static final long sStorage = 0x1L << 14;        // 0x4000
    public static final long sCopy = 0x1L << 15;           // 0x8000
    public static final long sToStringKeyOnly = 0x1L << 16;          // 0x0001 0000
    public static final long sGetId = 0x1L << 17;                    // 0x0002 0000
    public static final long sCopyAllProperties = 0x1L << 18;        // 0x0004 0000
    public static final long sCopyPrimaryKeyProperties = 0x1L << 19; // 0x0080 0000
    public static final long sCopyUnequalProperties = 0x1L << 20;    // 0x0010 0000
    public static final long sCopyDirtyProperties = 0x1L << 21;      // 0x0020 0000
    public static final long sHasDirtyProperties = 0x1L << 25;       // 0x0040 0000
    public static final long sEqualKeys = 0x1L << 22;                // 0x0080 0000
    public static final long sEqualProperties = 0x1L << 23;          // 0x0100 0000
    public static final long sCopyVersionProperty = 0x1L << 24;      // 0x0200 0000
    public static final long sMarkPropertiesClean = 0x1L << 26;      // 0x0400 0000
    public static final long sMarkAllPropertiesClean = 0x1L << 27;   // 0x0800 0000
    public static final long sMarkPropertiesDirty = 0x1L << 28;      // 0x1000 0000
    public static final long sMarkAllPropertiesDirty = 0x1L << 29;   // 0x2000 0000
    public static final long sStorableType = 0x1L << 30;             // 0x4000 0000

    public static final long ALL_SET_METHODS =       // 0x00000155;
            sSetId + sSetStringProp + sSetIntProp + sSetLongProp + sSetDoubleProp;
    public static final long ALL_GET_METHODS =       // 0x000200AA;
            sGetId + sGetStringProp + sGetIntProp + sGetLongProp + sGetDoubleProp;
    public static final long ALL_PRIMARY_KEYS = sSetId;     // 0x00000001;
    public static final long ALL_COPY_PROP_METHODS =  // 0x003C0000;
            sCopyAllProperties + sCopyPrimaryKeyProperties + sCopyUnequalProperties +
            sCopyDirtyProperties + sCopyVersionProperty;
    public static final long ALL_INTERFACE_METHODS = // 0x43C1FE00;
            sLoad + sTryLoad + sInsert + sUpdate + sDelete + sStorage + sCopy + sToStringKeyOnly +
            ALL_COPY_PROP_METHODS + sHasDirtyProperties + sEqualKeys + sEqualProperties +
            sMarkPropertiesClean + sMarkPropertiesDirty +
            sMarkAllPropertiesClean + sMarkAllPropertiesDirty + sStorableType;

    private Repository mRepository;
    private static int s_Ids = 0;

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestStorables.class);
    }

    public TestStorables(String s) {
        super(s);
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (mRepository != null) {
            // The test may have thrown exceptions which cause some
            // repositories to kick off asynchronous repairs. They will
            // immediately fail since the repository is about to be
            // closed. This just eliminates uninteresting errors from being
            // logged.
            try {
                RepairExecutor.waitForRepairsToFinish(10000);
            }
            catch (InterruptedException e) {
            }

            mRepository.close();
            mRepository = null;
        }
    }

    /**
     * Subclasses may override this method to change the repository.
     */
    protected Repository buildRepository(boolean isMaster) throws RepositoryException {
        return TestUtilities.buildTempRepository(isMaster);
    }

    /**
     * provide subsequent access to the repository so the tests can do fancy things if
     * interested.
     * @return
     */
    protected Repository getRepository() throws RepositoryException {
        if (mRepository == null) {
            mRepository = buildRepository(true);
        }
        return mRepository;
    }

    /**
     * Create a random ID to eliminate optimistic lock conflicts due to ID collisions
     * @param seed
     * @return
     */
    private int generateId(int seed) {
        return seed*10000 + ((int)System.currentTimeMillis()) + s_Ids++;
    }

    public void test_createAndRetrieve() throws Exception {
        Storage<StorableTestBasic> storageSB =
            getRepository().storageFor(StorableTestBasic.class);

        StorableTestBasic sb = storageSB.prepare();
        final int id = generateId(0);
        sb.setId(id);
        sb.setIntProp(1);
        sb.setLongProp(1);
        sb.setDoubleProp(1.1);
        sb.setStringProp("one");
        sb.setDate(new DateTime("2005-08-26T08:09:00.000"));
        sb.insert();

        StorableTestBasic sb_load = storageSB.prepare();
        sb_load.setId(id);
        sb_load.load();
        assertEquals(sb, sb_load);

        // Try re-inserting
        // First, make sure the system disallows setting pk for loaded object
        try {
            sb.setId(id);
            fail("successfully set pk on loaded object");
        }
        catch (Exception e) {
        }

        // Then the more common way: just create an identical new one
        StorableTestBasic sbr = storageSB.prepare();
        sbr.setId(id);
        sbr.setIntProp(1);
        sbr.setLongProp(1);
        sbr.setDoubleProp(1.1);
        sbr.setStringProp("one");
        sb.setDate(new DateTime("2005-08-26T08:09:00.000"));
        try {
            sbr.insert();
            fail("PK constraint violation ignored");
        }
        catch (UniqueConstraintException e) {
        }


        Storage<StorableTestMultiPK> storageMPK =
                getRepository().storageFor(StorableTestMultiPK.class);
        StorableTestMultiPK smpk = storageMPK.prepare();
        smpk.setIdPK(0);
        smpk.setStringPK("zero");
        smpk.setStringData("and some data");
        smpk.insert();

        StorableTestMultiPK smpk_load = storageMPK.prepare();
        smpk_load.setIdPK(0);
        smpk_load.setStringPK("zero");
        smpk_load.load();
        assertEquals(smpk, smpk_load);
    }

    public void test_storableStorableStates() throws Exception {

        Storage<StorableTestKeyValue> storageMinimal =
            getRepository().storageFor(StorableTestKeyValue.class);

        // Start by just putting some targets in the repository
        for (int i = 0; i < 10; i++) {
            insert(storageMinimal, 100+i, 200+i);
        }

        StorableTestKeyValue s = storageMinimal.prepare();
        StorableTestKeyValue s2 = storageMinimal.prepare();

        // State: unloaded
        // pk incomplete
        assertInoperable(s, "new - untouched");
        assertFalse(s.hasDirtyProperties());

        // new --set(pk)--> loadable incomplete
        s.setKey1(0);
        assertInoperable(s, "Loadable Incomplete");
        assertFalse(s.hasDirtyProperties());

        s.setKey1(101);
        assertInoperable(s, "loadable incomplete (2nd)");
        assertFalse(s.hasDirtyProperties());

        // loadable incomplete --pkFilled--> loadable ready
        s.setKey2(201);
        assertEquals(true, s.tryDelete());
        assertFalse(s.hasDirtyProperties());

        s.setKey1(102);
        s.setKey2(202);
        assertEquals(true, s.tryDelete());
        assertFalse(s.hasDirtyProperties());

        // loadable ready --load()--> loaded
        s.setKey1(103);
        s.setKey2(203);
        s.load();
        assertEquals(s.getValue1(), 1030);
        assertEquals(s.getValue2(), 20300);
        assertNoInsert(s, "written");
        assertNoSetPK(s, "written");
        assertFalse(s.hasDirtyProperties());

        s2.setKey1(103);
        s2.setKey2(203);
        assertFalse(s2.hasDirtyProperties());
        s2.load();
        assertFalse(s2.hasDirtyProperties());

        assertTrue(s.equalPrimaryKeys(s2));
        assertTrue(s.equalProperties(s2));
        assertEquals(s.storableType(), s2.storableType());
        assertTrue(s.equals(s2));
        assertEquals(s, s2);
        s.setValue1(11);
        s.setValue2(11111);
        assertEquals(true, s.tryUpdate());
        assertEquals(11, s.getValue1());
        assertEquals(11111, s.getValue2());
        s2.load();
        assertEquals(s, s2);

        StorableTestKeyValue s3 = storageMinimal.prepare();
        s.copyPrimaryKeyProperties(s3);
        s3.tryUpdate();
        assertEquals(s, s3);

        s.setValue2(222222);
        assertTrue(s.hasDirtyProperties());
        s.load();
        assertFalse(s.hasDirtyProperties());
        assertEquals(s, s2);

        // Update should return true, even though it probably didn't actually
        // touch the storage layer.
        assertEquals(true, s.tryUpdate());

        s.tryDelete();
        assertNoLoad(s, "deleted");
        // After delete, saved properties remain dirty.
        assertTrue(s.hasDirtyProperties());

        s.insert();
        assertFalse(s.hasDirtyProperties());
        s.load();
        assertFalse(s.hasDirtyProperties());
        assertEquals(s, s2);
    }

    public void test_storableInteractions() throws Exception {
        Storage<StorableTestBasic> storage = getRepository().storageFor(StorableTestBasic.class);
        StorableTestBasic s = storage.prepare();
        final int id = generateId(111);
        s.setId(id);
        s.initBasicProperties();
        assertTrue(s.hasDirtyProperties());
        s.insert();
        assertFalse(s.hasDirtyProperties());

        StorableTestBasic s2 = storage.prepare();
        s2.setId(id);
        s2.load();
        assertTrue(s2.equalPrimaryKeys(s));
        assertTrue(s2.equalProperties(s));
        assertTrue(s2.equals(s));

        StorableTestBasic s3 = storage.prepare();
        s3.setId(id);
        s3.tryDelete();

        // Should be able to re-insert.
        s2.insert();
        s2.load();
        assertTrue(s2.equalPrimaryKeys(s));
        assertTrue(s2.equalProperties(s));
        assertTrue(s2.equals(s));

        // Delete in preparation for next test.
        s3.tryDelete();

        s.setStringProp("updated by s");
        // The object is gone, we can't update it
        assertEquals(false, s.tryUpdate());
        // ...or load it
        try {
            s2.load();
            fail("shouldn't be able to load a deleted object");
        }
        catch (FetchException e) {
        }
    }

    public void test_assymetricStorable() throws Exception {
        try {
            Storage<StorableTestAssymetric> storage =
                    getRepository().storageFor(StorableTestAssymetric.class);
        }
        catch (Exception e) {
            fail("exception creating storable with assymetric concrete getter?" + e);
        }
        try {
            Storage<StorableTestAssymetricGet> storage =
                    getRepository().storageFor(StorableTestAssymetricGet.class);
            fail("Created assymetric storabl`e");
        }
        catch (Exception e) {
        }
        try {
            Storage<StorableTestAssymetricSet> storage =
                    getRepository().storageFor(StorableTestAssymetricSet.class);
            fail("Created assymetric storable");
        }
        catch (Exception e) {
        }
        try {
            Storage<StorableTestAssymetricGetSet> storage =
                    getRepository().storageFor(StorableTestAssymetricGetSet.class);
            fail("Created assymetric storable");
        }
        catch (Exception e) {
        }
    }

    private void assertNoSetPK(StorableTestKeyValue aStorableTestKeyValue,
                               String aState)
    {
        try {
            aStorableTestKeyValue.setKey1(1111);
            fail("'set pk' for '" + aState + "' succeeded");
        }
        catch (Exception e) {
        }
    }

    private void assertInoperable(Storable aStorable, String aState) {
        assertNoInsert(aStorable, aState);
        assertNoUpdate(aStorable, aState);
        assertNoLoad(aStorable, aState);
        assertNoDelete(aStorable, aState);
    }

    private void assertNoDelete(Storable aStorable, String aState) {
        try {
            aStorable.tryDelete();
            fail("'delete' for '" + aState + "' succeeded");
        }
        catch (PersistException e) {
        }
        catch (IllegalStateException e) {
        }
    }

    private void assertNoLoad(Storable aStorable, String aState) {
        try {
            aStorable.load();
            fail("'load' for '" + aState + "' succeeded");
        }
        catch (FetchException e) {
        }
        catch (IllegalStateException e) {
        }
    }

    private void assertNoInsert(Storable aStorable, String aState) {
        try {
            aStorable.insert();
            fail("'insert' for '" + aState + "' succeeded");
        }
        catch (PersistException e) {
        }
        catch (IllegalStateException e) {
        }
    }

    private void assertNoUpdate(Storable aStorable, String aState) {
        try {
            aStorable.tryUpdate();
            fail("'update' for '" + aState + "' succeeded");
        }
        catch (PersistException e) {
        }
        catch (IllegalStateException e) {
        }
    }

    private void insert(Storage<StorableTestKeyValue> aStorageMinimal,
                        int key1,
                        int key2) throws PersistException
    {
        StorableTestKeyValue s = aStorageMinimal.prepare();
        s.setKey1(key1);
        s.setKey2(key2);
        s.setValue1(key1*10);
        s.setValue2(key2*100);
        s.insert();
    }

    public void test_copyStorableProperties() throws Exception {
        Storage<StorableTestBasic> storage = getRepository().storageFor(StorableTestBasic.class);
        StorableTestBasic storable = storage.prepare();

        InvocationTracker tracker = new InvocationTracker("tracker", false);
        storable.copyAllProperties(tracker);
        // unloaded, untouched; nothing happens
        tracker.assertTrack(0);

        storable.setId(generateId(1));
        storable.setIntProp(1);
        storable.copyAllProperties(tracker);
        tracker.assertTrack(0x1 + 0x10);
        tracker.clearTracks();

        storable.initBasicProperties();
        storable.copyAllProperties(tracker);
        tracker.assertTrack(ALL_SET_METHODS);
        tracker.clearTracks();

        storable = storage.prepare();
        storable.copyPrimaryKeyProperties(tracker);
        tracker.assertTrack(0);
        storable.initPrimaryKeyProperties();
        storable.copyPrimaryKeyProperties(tracker);
        tracker.assertTrack(ALL_PRIMARY_KEYS);
        tracker.clearTracks();

        storable = storage.prepare();
        storable.copyUnequalProperties(tracker);
        tracker.assertTrack(0);
        storable.setIntProp(0);  // this will now be dirty, and equal
        storable.copyUnequalProperties(tracker);
        tracker.assertTrack(0x8);
        storable.setIntProp(1);  // this will now be dirty and not equal
        storable.copyUnequalProperties(tracker);
        tracker.assertTrack(0x8 | 0x10);

        // get a fresh one
        storable = storage.prepare();
        storable.setStringProp("hi");
        storable.setId(22);
        storable.copyPrimaryKeyProperties(tracker);
        storable.copyDirtyProperties(tracker);
        tracker.assertTrack(0x05);
    }

    public void test_copy() throws Exception {
        Storage<StorableTestBasic> storage = getRepository().storageFor(StorableTestBasic.class);
        StorableTestBasic storable = storage.prepare();

        storable.setId(5);
        storable.setStringProp("hello");
        storable.setIntProp(512);
        storable.setLongProp(45354L);
        storable.setDoubleProp(56734.234);

        Storable copy = storable.copy();

        assertEquals(storable.getClass(), copy.getClass());
        assertEquals(storable, copy);

        StorableTestBasic castedCopy = (StorableTestBasic) copy;

        assertEquals(storable.getId(), castedCopy.getId());
        assertEquals(storable.getStringProp(), castedCopy.getStringProp());
        assertEquals(storable.getIntProp(), castedCopy.getIntProp());
        assertEquals(storable.getLongProp(), castedCopy.getLongProp());
        assertEquals(storable.getDoubleProp(), castedCopy.getDoubleProp());
    }

    public void test_invalidStorables() throws Exception {
        try {
            getRepository().storageFor(StorableTestInvalid.class);
            fail("prepared invalid storable");
        }
        catch (RepositoryException e) {
        }
    }

    public void test_invalidPatterns() throws Exception {
        // Minimal -- try setting with no PK
        Storage<StorableTestMinimal> storageMinimal =
                getRepository().storageFor(StorableTestMinimal.class);
        StorableTestMinimal s = storageMinimal.prepare();
        assertNoInsert(s, "new (minimal)");

        s.setId(generateId(0));
        s.insert();

        // Basic -- try combinations of PK, fields
        // First, fill in all the fields but no PK
        Storage<StorableTestBasic> storageBasic =
                getRepository().storageFor(StorableTestBasic.class);
        StorableTestBasic sb = storageBasic.prepare();
        assertNoInsert(sb, "new (basic)");;

        sb.setIntProp(0);
        sb.setIntProp(1);
        sb.setLongProp(1);
        sb.setDoubleProp(1.1);
        sb.setStringProp("one");
        sb.setDate(new DateTime("2005-08-26T08:09:00.000"));
        assertNoInsert(sb, "SB: Storable incomplete (pkMissing)");

        sb.setId(generateId(2));
        sb.insert();

        // Now try leaving one of the fields empty.
        sb = storageBasic.prepare();
        final int id = generateId(3);
        sb.setId(id);
        sb.setIntProp(0);
        sb.setIntProp(1);
        sb.setLongProp(1);
        sb.setDoubleProp(1.1);
        try {
            sb.insert();
            fail();
        } catch (ConstraintException e) {
        }

        sb = storageBasic.prepare();
        sb.setId(id);
        try {
            sb.load();
            fail();
        } catch (FetchNoneException e) {
        }
    }

    public void test_nonDestructiveUpdate() throws Exception {
        Storage<StorableTestBasic> storage = getRepository().storageFor(StorableTestBasic.class);
        StorableTestBasic s = storage.prepare();

        int id = generateId(3943945);
        s.setId(id);
        s.setStringProp("hello");
        s.setIntProp(56);
        s.setLongProp(99999999999999999L);
        s.setDoubleProp(Double.NaN);

        s.insert();

        s = storage.prepare();

        s.setId(id);
        s.setIntProp(100);
        assertEquals(true, s.tryUpdate());

        assertEquals("hello", s.getStringProp());
        assertEquals(100, s.getIntProp());
        assertEquals(99999999999999999L, s.getLongProp());
        assertEquals(Double.NaN, s.getDoubleProp());

        s = storage.prepare();

        s.setId(id);
        s.load();

        assertEquals("hello", s.getStringProp());
        assertEquals(100, s.getIntProp());
        assertEquals(99999999999999999L, s.getLongProp());
        assertEquals(Double.NaN, s.getDoubleProp());
    }

    public void test_updateLoadSideEffect() throws Exception {
        Storage<StorableTestBasic> storage = getRepository().storageFor(StorableTestBasic.class);

        StorableTestBasic s = storage.prepare();
        final int id = generateId(500);
        s.setId(id);
        s.setStringProp("hello");
        s.setIntProp(10);
        s.setLongProp(123456789012345L);
        s.setDoubleProp(Double.POSITIVE_INFINITY);
        s.insert();

        s = storage.prepare();
        s.setId(id);

        assertEquals(id, s.getId());
        assertEquals(0, s.getIntProp());
        assertEquals(0L, s.getLongProp());
        assertEquals(0.0, s.getDoubleProp());

        assertFalse(s.hasDirtyProperties());

        // Even if nothing was updated, must load fresh copy.
        assertTrue(s.tryUpdate());
        assertEquals(id, s.getId());
        assertEquals(10, s.getIntProp());
        assertEquals(123456789012345L, s.getLongProp());
        assertEquals(Double.POSITIVE_INFINITY, s.getDoubleProp());
    }

    public void test_versioning() throws Exception {
        Storage<StorableVersioned> storage = getRepository().storageFor(StorableVersioned.class);

        StorableVersioned s = storage.prepare();
        s.setID(500);
        s.setValue("hello");
        try {
            // Require version property to be set.
            s.tryUpdate();
            fail();
        } catch (IllegalStateException e) {
        }

        s.setVersion(1);
        try {
            // Cannot update that which does not exist.
            s.update();
            fail();
        } catch (PersistNoneException e) {
        }

        s.insert();

        s.setVersion(2);
        try {
            // Record version mismatch.
            s.tryUpdate();
            fail();
        } catch (OptimisticLockException e) {
        }

        s.setVersion(1);
        s.setValue("world");
        s.tryUpdate();

        assertEquals(2, s.getVersion());
        assertEquals("world", s.getValue());

        // Even if no properties changed, update increases version.
        assertEquals(true, s.tryUpdate());
        assertEquals(3, s.getVersion());

        // Simple test to ensure that version property doesn't need to be
        // dirtied.
        s = storage.prepare();
        s.setID(500);
        s.load();
        s.setValue("hello");
        assertTrue(s.tryUpdate());
    }

    public void test_versioningWithLong() throws Exception {
        Storage<StorableVersionedWithLong> storage =
            getRepository().storageFor(StorableVersionedWithLong.class);

        StorableVersionedWithLong s = storage.prepare();
        s.setID(500);
        s.setValue("hello");
        try {
            // Require version property to be set.
            s.tryUpdate();
            fail();
        } catch (IllegalStateException e) {
        }

        s.setVersion(1);
        try {
            // Cannot update that which does not exist.
            s.update();
            fail();
        } catch (PersistNoneException e) {
        }

        s.insert();

        s.setVersion(2);
        try {
            // Record version mismatch.
            s.tryUpdate();
            fail();
        } catch (OptimisticLockException e) {
        }

        s.setVersion(1);
        s.setValue("world");
        s.tryUpdate();

        assertEquals(2, s.getVersion());
        assertEquals("world", s.getValue());

        // Even if no properties changed, update increases version.
        assertEquals(true, s.tryUpdate());
        assertEquals(3, s.getVersion());
    }

    public void test_versioningWithObj() throws Exception {
        Storage<StorableVersionedWithObj> storage =
            getRepository().storageFor(StorableVersionedWithObj.class);

        StorableVersionedWithObj s = storage.prepare();
        s.setID(generateId(500));
        s.setValue("hello");
        try {
            // Require version property to be set.
            s.tryUpdate();
            fail();
        } catch (IllegalStateException e) {
        }

        s.setVersion(null);
        try {
            // Cannot update that which does not exist.
            s.update();
            fail();
        } catch (PersistNoneException e) {
        }

        s.insert();

        assertNull(s.getVersion());

        s.setVersion(2);
        try {
            // Record version mismatch.
            s.tryUpdate();
            fail();
        } catch (OptimisticLockException e) {
        }

        s.setVersion(null);
        s.setValue("world");
        s.tryUpdate();

        assertEquals((Integer) 1, s.getVersion());
        assertEquals("world", s.getValue());

        s.setValue("value");
        s.tryUpdate();

        assertEquals((Integer) 2, s.getVersion());
        assertEquals("value", s.getValue());

        // Even if no properties changed, update increases version.
        assertEquals(true, s.tryUpdate());
        assertEquals((Integer) 3, s.getVersion());
    }

    public void test_versioningWithLongObj() throws Exception {
        Storage<StorableVersionedWithLongObj> storage =
            getRepository().storageFor(StorableVersionedWithLongObj.class);

        StorableVersionedWithLongObj s = storage.prepare();
        s.setID(500);
        s.setValue("hello");
        try {
            // Require version property to be set.
            s.tryUpdate();
            fail();
        } catch (IllegalStateException e) {
        }

        s.setVersion(null);
        try {
            // Cannot update that which does not exist.
            s.update();
            fail();
        } catch (PersistNoneException e) {
        }

        s.insert();

        assertNull(s.getVersion());

        s.setVersion(2L);
        try {
            // Record version mismatch.
            s.tryUpdate();
            fail();
        } catch (OptimisticLockException e) {
        }

        s.setVersion(null);
        s.setValue("world");
        s.tryUpdate();

        assertEquals((Long) 1L, s.getVersion());
        assertEquals("world", s.getValue());

        s.setValue("value");
        s.tryUpdate();

        assertEquals((Long) 2L, s.getVersion());
        assertEquals("value", s.getValue());

        // Even if no properties changed, update increases version.
        assertEquals(true, s.tryUpdate());
        assertEquals((Long) 3L, s.getVersion());
    }

    public void test_initialVersion() throws Exception {
        Storage<StorableVersioned> storage = getRepository().storageFor(StorableVersioned.class);

        StorableVersioned s = storage.prepare();
        s.setID(987);
        s.setValue("hello");
        assertEquals(0, s.getVersion());
        s.insert();
        assertEquals(1, s.getVersion());

        s = storage.prepare();
        s.setID(12345);
        s.setValue("world");
        assertEquals(0, s.getVersion());
        s.setVersion(56);
        assertEquals(56, s.getVersion());
        s.insert();
        assertEquals(56, s.getVersion());
    }

    public void test_initialVersionWithLong() throws Exception {
        Storage<StorableVersionedWithLong> storage =
            getRepository().storageFor(StorableVersionedWithLong.class);

        StorableVersionedWithLong s = storage.prepare();
        s.setID(987);
        s.setValue("hello");
        assertEquals(0, s.getVersion());
        s.insert();
        assertEquals(1, s.getVersion());

        s = storage.prepare();
        s.setID(12345);
        s.setValue("world");
        assertEquals(0, s.getVersion());
        s.setVersion(56);
        assertEquals(56, s.getVersion());
        s.insert();
        assertEquals(56, s.getVersion());
    }

    public void test_initialVersionWithObj() throws Exception {
        Storage<StorableVersionedWithObj> storage =
            getRepository().storageFor(StorableVersionedWithObj.class);

        StorableVersionedWithObj s = storage.prepare();
        s.setID(987);
        s.setValue("hello");
        assertNull(s.getVersion());
        s.insert();
        assertEquals((Integer) 1, s.getVersion());

        s = storage.prepare();
        s.setID(12345);
        s.setValue("world");
        assertNull(s.getVersion());
        s.setVersion(56);
        assertEquals((Integer) 56, s.getVersion());
        s.insert();
        assertEquals((Integer) 56, s.getVersion());
    }

    public void test_initialVersionWithLongObj() throws Exception {
        Storage<StorableVersionedWithLongObj> storage =
            getRepository().storageFor(StorableVersionedWithLongObj.class);

        StorableVersionedWithLongObj s = storage.prepare();
        s.setID(987);
        s.setValue("hello");
        assertNull(s.getVersion());
        s.insert();
        assertEquals((Long) 1L, s.getVersion());

        s = storage.prepare();
        s.setID(12345);
        s.setValue("world");
        assertNull(s.getVersion());
        s.setVersion(56L);
        assertEquals((Long) 56L, s.getVersion());
        s.insert();
        assertEquals((Long) 56L, s.getVersion());
    }

    public void test_versioningMissingRecord() throws Exception {
        Storage<StorableVersioned> storage = getRepository().storageFor(StorableVersioned.class);

        StorableVersioned s = storage.prepare();
        s.setID(500);
        s.setValue("hello");
        s.insert();

        // Now delete it from under our feet.
        StorableVersioned s2 = storage.prepare();
        s2.setID(500);
        s2.delete();

        s.setValue("world");
        s.tryUpdate();

        s.insert();

        // Delete it again.
        s2.delete();

        // Update without changing and properties must still reload, which should fail.
        assertFalse(s.tryUpdate());
    }

    public void test_versioningDisabled() throws Exception {
        // Make sure repository works properly when configured as non-master.
        Repository repo = buildRepository(false);
        Storage<StorableVersioned> storage = repo.storageFor(StorableVersioned.class);

        StorableVersioned s = storage.prepare();
        s.setID(500);
        s.setValue("hello");
        try {
            // Require version property to be set.
            s.tryUpdate();
            fail();
        } catch (IllegalStateException e) {
        }

        s.setVersion(1);
        assertEquals(false, s.tryUpdate());

        s.insert();

        s.setVersion(2);
        assertEquals(true, s.tryUpdate());

        s.setVersion(1);
        s.setValue("world");
        s.tryUpdate();

        assertEquals(1, s.getVersion());
        assertEquals("world", s.getValue());

        RepairExecutor.waitForRepairsToFinish(10000);

        repo.close();
        repo = null;
    }

    public void test_derivedVersion() throws Exception {
        Storage<WithDerivedVersion> storage = getRepository().storageFor(WithDerivedVersion.class);

        WithDerivedVersion s = storage.prepare();
        s.setID(1);
        s.setName("john");
        s.setValue(2);
        s.insert();
        assertEquals(2, s.getVersion());

        try {
            s.setName("bob");
            s.setValue(2); // dirty the property version derives from
            s.update();
            fail();
        } catch (OptimisticLockException e) {
        }

        s.setValue(3);
        s.update();

        try {
            s.load();
            s.setName("fred");
            s.setValue(1);
            s.update();
            fail();
        } catch (OptimisticLockException e) {
        }

        s = storage.prepare();
        s.setID(1);
        s.setName("fred");
        s.setValue(100);
        s.update();
    }

    public void test_derivedFloatVersion() throws Exception {
        Storage<WithDerivedFloatVersion> storage =
            getRepository().storageFor(WithDerivedFloatVersion.class);

        WithDerivedFloatVersion s = storage.prepare();
        s.setID(1);
        s.setName("john");
        s.setValue(2.1f);
        s.insert();
        assertEquals(2.1f, s.getVersion());

        try {
            s.setName("bob");
            s.setValue(2.1f); // dirty the property version derives from
            s.update();
            fail();
        } catch (OptimisticLockException e) {
        }

        s.setValue(2.12f);
        s.update();

        try {
            s.load();
            s.setName("fred");
            s.setValue(1);
            s.update();
            fail();
        } catch (OptimisticLockException e) {
        }

        s = storage.prepare();
        s.setID(1);
        s.setName("fred");
        s.setValue(100);
        s.update();

        s.setValue(Float.NaN);
        s.setName("sara");
        s.update();

        s.setValue(3.14f);
        s.setName("conner");
        s.update();
    }

    public void test_derivedDoubleObjVersion() throws Exception {
        Storage<WithDerivedDoubleObjVersion> storage =
            getRepository().storageFor(WithDerivedDoubleObjVersion.class);

        WithDerivedDoubleObjVersion s = storage.prepare();
        s.setID(1);
        s.setName("john");
        s.setValue(2.1);
        s.insert();
        assertEquals(2.1, s.getVersion());

        try {
            s.setName("bob");
            s.setValue(2.1); // dirty the property version derives from
            s.update();
            fail();
        } catch (OptimisticLockException e) {
        }

        s.setValue(2.12);
        s.update();

        try {
            s.load();
            s.setName("fred");
            s.setValue(1.0);
            s.update();
            fail();
        } catch (OptimisticLockException e) {
        }

        s = storage.prepare();
        s.setID(1);
        s.setName("fred");
        s.setValue(100.0);
        s.update();

        s.setValue(Double.NaN);
        s.setName("sara");
        s.update();

        s.setValue(3.14);
        s.setName("conner");
        s.update();

        s.setValue(null);
        s.setName("steve");
        s.update();

        s.setValue(1.5);
        s.setName("cathy");
        s.update();
    }

    public void test_derivedStringVersion() throws Exception {
        Storage<WithDerivedStringVersion> storage =
            getRepository().storageFor(WithDerivedStringVersion.class);

        WithDerivedStringVersion s = storage.prepare();
        s.setID(1);
        s.setName("john");
        s.setValue("a");
        s.insert();
        assertEquals("a", s.getVersion());

        try {
            s.setName("bob");
            s.setValue("a"); // dirty the property version derives from
            s.update();
            fail();
        } catch (OptimisticLockException e) {
        }

        s.setValue("b");
        s.update();

        try {
            s.load();
            s.setName("fred");
            s.setValue("a");
            s.update();
            fail();
        } catch (OptimisticLockException e) {
        }

        s = storage.prepare();
        s.setID(1);
        s.setName("fred");
        s.setValue("c");
        s.update();

        s.setValue(null);
        s.setName("steve");
        s.update();

        s.setValue("a");
        s.setName("cathy");
        s.update();
    }

    public void test_sequences() throws Exception {
        Storage<StorableSequenced> storage = getRepository().storageFor(StorableSequenced.class);

        StorableSequenced seq = storage.prepare();
        seq.setData("hello");
        seq.insert();

        assertEquals(1L, seq.getID());
        assertEquals(1, seq.getSomeInt());
        assertEquals(Integer.valueOf(1), seq.getSomeIntegerObj());
        assertEquals(1L, seq.getSomeLong());
        assertEquals(Long.valueOf(1L), seq.getSomeLongObj());
        assertEquals("1", seq.getSomeString());
        assertEquals("hello", seq.getData());

        seq = storage.prepare();
        seq.setData("foo");
        seq.insert();

        assertEquals(2L, seq.getID());
        assertEquals(2, seq.getSomeInt());
        assertEquals(Integer.valueOf(2), seq.getSomeIntegerObj());
        assertEquals(2L, seq.getSomeLong());
        assertEquals(Long.valueOf(2L), seq.getSomeLongObj());
        assertEquals("2", seq.getSomeString());
        assertEquals("foo", seq.getData());
        
        seq = storage.prepare();
        seq.setSomeInt(100);
        seq.setSomeLongObj(null);
        seq.setData("data");
        seq.insert();

        assertEquals(3L, seq.getID());
        assertEquals(100, seq.getSomeInt());
        assertEquals(null, seq.getSomeLongObj());

        seq = storage.prepare();
        seq.setData("world");
        seq.insert();

        assertEquals(4L, seq.getID());
        assertEquals(3, seq.getSomeInt());
        assertEquals(Integer.valueOf(4), seq.getSomeIntegerObj());
        assertEquals(4L, seq.getSomeLong());
        assertEquals(Long.valueOf(3L), seq.getSomeLongObj());
        assertEquals("4", seq.getSomeString());
    }

    public void test_sequenceRollback() throws Exception {
        // Make sure sequence does not rollback with the main
        // transaction. Sequences must always increase, even if enclosing
        // transaction rolls back. Otherwise, you get race conditions and
        // sequence values might get re-used.

        Storage<SequenceAndAltKey> storage = getRepository().storageFor(SequenceAndAltKey.class);

        SequenceAndAltKey obj = storage.prepare();
        obj.setName("foo");
        obj.setData("hello");
        obj.insert();

        int lastID = obj.getID();

        for (int i=0; i<10000; i++) {
            obj = storage.prepare();
            obj.setName("foo");
            obj.setData("world");
            // Alternate key constraint.
            assertFalse(obj.tryInsert());
            // Sequence must always increase, even if insert failed.
            assertTrue(obj.getID() > lastID);
            lastID = obj.getID();
        }
    }

    public void test_oldIndexEntryDeletion() throws Exception {
        // Very simple test that ensures that old index entries are deleted
        // when the master record is updated. There is no guarantee that the
        // chosen repository supports indexes, and there is no guarantee that
        // it is selecting the desired index. Since the index set is simple and
        // so are the queries, I think it is safe to assume that the selected
        // index is what I expect it to be. Repositories that support
        // custom indexing should have more rigorous tests.

        Storage<StorableTestBasicIndexed> storage =
            getRepository().storageFor(StorableTestBasicIndexed.class);

        StorableTestBasicIndexed s = storage.prepare();
        final int id1 = generateId(1);
        s.setId(id1);
        s.setStringProp("hello");
        s.setIntProp(3);
        s.setLongProp(4);
        s.setDoubleProp(5);
        s.insert();

        s = storage.prepare();
        final int id6 = generateId(6);
        s.setId(id6);
        s.setStringProp("hello");
        s.setIntProp(8);
        s.setLongProp(9);
        s.setDoubleProp(10);
        s.insert();

        s = storage.prepare();
        final int id11 = generateId(11);
        s.setId(id11);
        s.setStringProp("world");
        s.setIntProp(3);
        s.setLongProp(14);
        s.setDoubleProp(15);
        s.insert();

        // First verify that queries report what we expect. Don't perform an
        // orderBy on query, as that might interfere with index selection.
        Query<StorableTestBasicIndexed> q = storage.query("stringProp = ?").with("hello");
        List<StorableTestBasicIndexed> list = q.fetch().toList();
        assertEquals(2, list.size());
        if (list.get(0).getId() == id1) {
            assertEquals(id6, list.get(1).getId());
        } else {
            assertEquals(id6, list.get(0).getId());
        }

        q = storage.query("stringProp = ?").with("world");
        list = q.fetch().toList();
        assertEquals(1, list.size());
        assertEquals(id11, list.get(0).getId());

        q = storage.query("intProp = ?").with(3);
        list = q.fetch().toList();
        assertEquals(2, list.size());
        if (list.get(0).getId() == id1) {
            assertEquals(id11, list.get(1).getId());
        } else {
            assertEquals(id11, list.get(0).getId());
        }

        // Now update and verify changes to query results.
        s = storage.prepare();
        s.setId(id1);
        s.load();
        s.setStringProp("world");
        s.tryUpdate();

        q = storage.query("stringProp = ?").with("hello");
        list = q.fetch().toList();
        assertEquals(1, list.size());
        assertEquals(id6, list.get(0).getId());

        q = storage.query("stringProp = ?").with("world");
        list = q.fetch().toList();
        assertEquals(2, list.size());
        if (list.get(0).getId() == id1) {
            assertEquals(id11, list.get(1).getId());
        } else {
            assertEquals(id11, list.get(0).getId());
        }

        q = storage.query("intProp = ?").with(3);
        list = q.fetch().toList();
        assertEquals(2, list.size());
        if (list.get(0).getId() == id1) {
            assertEquals(id11, list.get(1).getId());
        } else {
            assertEquals(id11, list.get(0).getId());
        }
    }

    public void test_falseDoubleUpdate() throws Exception {
        Storage<StorableTestBasic> storage = getRepository().storageFor(StorableTestBasic.class);

        StorableTestBasic s = storage.prepare();
        s.setId(56789);

        assertFalse(s.tryUpdate());
        assertFalse(s.tryUpdate());
    }

    public void test_dateTimeIndex() throws Exception {
        Storage<StorableDateIndex> storage = getRepository().storageFor(StorableDateIndex.class);

        DateTimeZone original = DateTimeZone.getDefault();
        // Set time zone different than defined in storable.
        DateTimeZone.setDefault(DateTimeZone.forID("America/Los_Angeles"));
        try {
            DateTime now = new DateTime();

            StorableDateIndex sdi = storage.prepare();
            sdi.setID(1);
            sdi.setOrderDate(now);
            sdi.insert();

            sdi.load();

            assertEquals(now.getMillis(), sdi.getOrderDate().getMillis());
            // Time zones will differ, since adapter is applied upon load.
            assertFalse(now.equals(sdi.getOrderDate()));

            Query<StorableDateIndex> query = storage.query("orderDate=?").with(now);
            // Tests that adapter is applied to index. Otherwise, consistency
            // check will reject loaded storable.
            StorableDateIndex sdi2 = query.tryLoadOne();
            assertNotNull(sdi2);
        } finally {
            DateTimeZone.setDefault(original);
        }
    }

    public void test_derivedFunctionIndex() throws Exception {
        Storage<WithFunctionIndex> storage = getRepository().storageFor(WithFunctionIndex.class);

        WithFunctionIndex s = storage.prepare();
        s.setID(1);
        s.setSum(10);
        s.setCount(20);
        s.insert();

        s = storage.prepare();
        s.setID(2);
        s.setSum(20);
        s.setCount(40);
        s.insert();

        s = storage.prepare();
        s.setID(3);
        s.setSum(20);
        s.setCount(0);
        s.insert();

        s = storage.prepare();
        s.setID(4);
        s.setSum(0);
        s.setCount(0);
        s.insert();

        s = storage.prepare();
        s.setID(5);
        s.setSum(10);
        s.setCount(10);
        s.insert();

        Query<WithFunctionIndex> query = storage.query("average = ?");
        Query<WithFunctionIndex> q2 = storage.query("averageObject = ?");

        assertEquals(2, query.with(0.5).count());
        assertEquals(3, query.with(20.0 / 0).loadOne().getID());
        assertEquals(4, query.with(0.0 / 0).loadOne().getID());
        assertEquals(5, query.with(1.0).loadOne().getID());

        assertEquals(2, q2.with(0.5).count());
        assertEquals(3, q2.with(20.0 / 0).loadOne().getID());
        assertEquals(4, q2.with(0.0 / 0).loadOne().getID());
        assertEquals(5, q2.with(1.0).loadOne().getID());

        s = storage.prepare();
        s.setID(5);
        s.setSum(2);
        s.update();

        assertEquals(0, query.with(1.0).count());
        assertEquals(5, query.with(0.2).loadOne().getID());

        s.delete();

        assertEquals(0, query.with(1.0).count());
        assertEquals(0, query.with(0.2).count());
    }

    public void test_derivedJoinIndex() throws Exception {
        Storage<WithDerivedChainA> aStorage = getRepository().storageFor(WithDerivedChainA.class);
        Storage<WithDerivedChainB> bStorage = getRepository().storageFor(WithDerivedChainB.class);
        Storage<WithDerivedChainC> cStorage = getRepository().storageFor(WithDerivedChainC.class);
        Storage<WithDerivedChainD> dStorage = getRepository().storageFor(WithDerivedChainD.class);

        int aid = 101;
        int bid = 201;
        int cid = 301;
        int did = 401;

        WithDerivedChainD d = dStorage.prepare();
        // Insert later
        //d.setDid(did);
        //d.setName("dee");
        //d.insert();
        d = dStorage.prepare();
        d.setDid(did + 1);
        d.setName("dee2");
        d.insert();

        WithDerivedChainC c = cStorage.prepare();
        c.setCid(cid);
        c.setName("cee");
        c.setDid(did);
        c.insert();
        c = cStorage.prepare();
        c.setCid(cid + 1);
        c.setName("cee2");
        c.setDid(did + 1);
        c.insert();

        WithDerivedChainB b = bStorage.prepare();
        b.setBid(bid);
        b.setName("bee");
        b.setCid(cid);
        b.insert();
        b = bStorage.prepare();
        b.setBid(bid + 1);
        b.setName("bee2");
        b.setCid(cid + 1);
        b.insert();

        WithDerivedChainA a = aStorage.prepare();
        a.setAid(aid);
        a.setName("aye");
        a.setBid(bid);
        try {
            // Insert fails because d doesn't exist.
            a.insert();
            fail();
        } catch (PersistException e) {
        }

        d = dStorage.prepare();
        d.setDid(did);
        d.setName("dee");
        d.insert();

        a.insert();

        a = aStorage.prepare();
        a.setAid(aid + 1);
        a.setName("aye2");
        a.setBid(bid + 1);
        a.insert();

        Query<WithDerivedChainA> aQuery = aStorage.query("DName = ?");

        assertEquals(101, aQuery.with("dee").loadOne().getAid());
        assertEquals(102, aQuery.with("dee2").loadOne().getAid());

        a.delete();

        assertEquals(101, aQuery.with("dee").loadOne().getAid());
        assertNull(aQuery.with("dee2").tryLoadOne());

        d.setName("dee!!!");
        d.update();

        assertNull(aQuery.with("dee").tryLoadOne());
        assertEquals(101, aQuery.with("dee!!!").loadOne().getAid());
    }

    public void test_joinCache() throws Exception {
        Storage<UserAddress> uaStorage = getRepository().storageFor(UserAddress.class);
        Storage<UserInfo> uiStorage = getRepository().storageFor(UserInfo.class);

        UserAddress addr = uaStorage.prepare();
        addr.setAddressID(5);
        addr.setLine1("123");
        addr.setCity("Seattle");
        addr.setState("WA");
        addr.setCountry("USA");
        addr.insert();

        UserInfo user = uiStorage.prepare();
        user.setUserID(1);
        user.setStateID(1);
        user.setFirstName("John");
        user.setLastName("Smith");
        user.setAddress(addr);

        assertEquals("Seattle", user.getAddress().getCity());

        user.insert();

        addr.setCity("Bellevue");
        addr.tryUpdate();

        // Should still refer to same address instance.
        assertEquals("Bellevue", user.getAddress().getCity());

        UserAddress addr2 = uaStorage.prepare();
        addr2.setAddressID(5);
        addr2.setCity("Kirkland");
        addr2.tryUpdate();

        // Should still refer to same address instance.
        assertEquals("Bellevue", user.getAddress().getCity());

        // Force reload of user should flush cache.
        user.load();

        assertEquals("Kirkland", user.getAddress().getCity());

        addr2.setCity("Redmond");
        addr2.tryUpdate();

        // Should still refer to same address instance.
        assertEquals("Kirkland", user.getAddress().getCity());

        // Update of user should flush cache (even if nothing changed)
        assertEquals(true, user.tryUpdate());

        assertEquals("Redmond", user.getAddress().getCity());

        addr2.setCity("Renton");
        addr2.tryUpdate();

        // Should still refer to same address instance.
        assertEquals("Redmond", user.getAddress().getCity());

        // Update of user should flush cache (when something changed)
        user.setFirstName("Jim");
        assertEquals(true, user.tryUpdate());

        assertEquals("Renton", user.getAddress().getCity());

        addr2.setCity("Tacoma");
        addr2.tryUpdate();

        // Should still refer to same address instance.
        assertEquals("Renton", user.getAddress().getCity());

        // Delete of user should flush cache
        assertEquals(true, user.tryDelete());

        assertEquals("Tacoma", user.getAddress().getCity());

        addr2.setCity("Shoreline");
        addr2.tryUpdate();

        // Should still refer to same address instance.
        assertEquals("Tacoma", user.getAddress().getCity());

        // Failed load of user should flush cache
        assertEquals(false, user.tryLoad());

        assertEquals("Shoreline", user.getAddress().getCity());

        addr2.setCity("Vancouver");
        addr2.tryUpdate();

        // Should still refer to same address instance.
        assertEquals("Shoreline", user.getAddress().getCity());

        // Insert of user should flush cache
        user.insert();

        assertEquals("Vancouver", user.getAddress().getCity());
    }

    public void test_updateReload() throws Exception {
        Storage<UserInfo> uiStorage = getRepository().storageFor(UserInfo.class);

        UserInfo user = uiStorage.prepare();
        user.setUserID(1);
        user.setStateID(1);
        user.setFirstName("John");
        user.setLastName("Smith");
        user.setAddressID(0);
        user.insert();

        UserInfo user2 = uiStorage.prepare();
        user2.setUserID(1);
        user2.setFirstName("Jim");
        user2.tryUpdate();

        assertEquals("John", user.getFirstName());
        assertTrue(user.tryUpdate());
        assertEquals("Jim", user.getFirstName());

        user2.setFirstName("Bob");
        user2.tryUpdate();

        assertEquals("Jim", user.getFirstName());
        user.setLastName("Jones");
        user.tryUpdate();
        assertEquals("Bob", user.getFirstName());
        assertEquals("Jones", user.getLastName());
    }

    public void test_deleteState() throws Exception {
        Storage<UserInfo> uiStorage = getRepository().storageFor(UserInfo.class);

        UserInfo user = uiStorage.prepare();
        user.setUserID(1);
        user.setStateID(1);
        user.setFirstName("John");
        user.setLastName("Smith");
        user.setAddressID(0);
        user.insert();

        UserInfo user2 = uiStorage.prepare();
        user2.setUserID(1);
        user2.tryDelete();

        assertFalse(user.tryLoad());

        // Should be able to change pk now.
        user.setUserID(2);
        assertFalse(user.tryUpdate());
        user.setUserID(1);
        user.insert();

        user2.tryDelete();

        assertFalse(user.tryUpdate());

        // Should be able to change pk now.
        user.setUserID(2);
        assertFalse(user.tryUpdate());
        user.setUserID(1);
        user.insert();
        user2.tryDelete();

        user.setFirstName("Jim");
        assertFalse(user.tryUpdate());

        // Should be able to change pk now.
        user.setUserID(2);
        assertFalse(user.tryUpdate());
        user.setUserID(1);
        user.insert();
        assertEquals("Jim", user.getFirstName());
    }

    public void test_deleteUpdate() throws Exception {
        Storage<UserInfo> uiStorage = getRepository().storageFor(UserInfo.class);

        UserInfo user = uiStorage.prepare();
        user.setUserID(1);
        user.setStateID(1);
        user.setFirstName("John");
        user.setLastName("Smith");
        user.setAddressID(0);
        user.insert();

        // Just want to change first name now
        user.setFirstName("Bob");

        // Concurrently, someone else deletes the user
        UserInfo user2 = uiStorage.prepare();
        user2.setUserID(1);
        assertTrue(user2.tryDelete());

        assertFalse(user.tryUpdate());

        // Update failed... perhaps we should try again... (wrong decision)

        // Concurrently, someone inserts a different user with re-used id. (wrong decision)
        user2 = uiStorage.prepare();
        user2.setUserID(1);
        user2.setStateID(1);
        user2.setFirstName("Mike");
        user2.setLastName("Jones");
        user2.setAddressID(0);
        user2.insert();

        // Trying the failed update again should totally blow away the sneaked in insert.
        assertTrue(user.tryUpdate());

        assertEquals("Bob", user.getFirstName());
        assertEquals("Smith", user.getLastName());

        // The failed update earlier dirtied all the properties, including ones
        // that were not modified. As a result, the second update replaces all
        // properties. This is a special edge case destructive update for which
        // there is no clear solution. If just the one property was changed as
        // instructed earlier, then the record would have been corrupted. This
        // feels much worse. This can still happen if the record was not
        // originally fully clean.

        // The cause of the error is the user creating a new record with a
        // re-used id. This problem could be prevented using transactions, or
        // it might be detected with optimistic locking.
    }

    public void test_deleteOne() throws Exception {
        Storage<UserInfo> uiStorage = getRepository().storageFor(UserInfo.class);

        UserInfo user = uiStorage.prepare();
        user.setUserID(1);
        user.setStateID(1);
        user.setFirstName("Bob");
        user.setLastName("Smith");
        user.setAddressID(0);
        user.insert();

        user = uiStorage.prepare();
        user.setUserID(2);
        user.setStateID(1);
        user.setFirstName("Bob");
        user.setLastName("Jones");
        user.setAddressID(0);
        user.insert();
        
        user = uiStorage.prepare();
        user.setUserID(3);
        user.setStateID(1);
        user.setFirstName("Indiana");
        user.setLastName("Jones");
        user.setAddressID(0);
        user.insert();

        try {
            uiStorage.query("lastName = ?").with("Jones").deleteOne();
            fail();
        } catch (PersistMultipleException e) {
        }

        List<UserInfo> list = uiStorage.query().fetch().toList();
        assertEquals(3, list.size());

        uiStorage.query("lastName = ? & firstName = ?").with("Jones").with("Bob").deleteOne();

        list = uiStorage.query().orderBy("userID").fetch().toList();
        assertEquals(2, list.size());

        assertEquals("Bob", list.get(0).getFirstName());
        assertEquals("Smith", list.get(0).getLastName());
        assertEquals("Indiana", list.get(1).getFirstName());
        assertEquals("Jones", list.get(1).getLastName());
    }

    public void test_triggers() throws Exception {
        Storage<StorableVersioned> storage = getRepository().storageFor(StorableVersioned.class);

        class InsertTrigger extends Trigger<StorableVersioned> {
            Object mState;

            @Override
            public Object beforeInsert(StorableVersioned s) {
                assertEquals(1, s.getID());
                mState = new Object();
                return mState;
            }

            @Override
            public void afterInsert(StorableVersioned s, Object state) {
                assertEquals(1, s.getID());
                assertEquals(mState, state);
            }
        };

        InsertTrigger it = new InsertTrigger();

        assertTrue(storage.addTrigger(it));

        StorableVersioned s = storage.prepare();
        s.setID(1);
        s.setValue("value");
        s.insert();

        assertTrue(it.mState != null);

        assertTrue(storage.removeTrigger(it));


        class LoadTrigger extends Trigger<StorableVersioned> {
            boolean mCalled;

            @Override
            public void afterLoad(StorableVersioned s) {
                assertEquals(1, s.getID());
                mCalled = true;
            }
        };

        LoadTrigger lt = new LoadTrigger();

        assertTrue(storage.addTrigger(lt));

        s.load();

        assertTrue(lt.mCalled);

        // Reset for query.
        lt.mCalled = false;
        storage.query("value = ?").with("value").fetch().toList();

        assertTrue(lt.mCalled);

        assertTrue(storage.removeTrigger(lt));


        class UpdateTrigger extends Trigger<StorableVersioned> {
            Object mState;
            int mVersion;

            @Override
            public Object beforeUpdate(StorableVersioned s) {
                assertEquals(1, s.getID());
                mState = new Object();
                mVersion = s.getVersion();
                return mState;
            }

            @Override
            public void afterUpdate(StorableVersioned s, Object state) {
                assertEquals(1, s.getID());
                assertEquals(mState, state);
                assertEquals(mVersion + 1, s.getVersion());
            }
        };

        UpdateTrigger ut = new UpdateTrigger();

        assertTrue(storage.addTrigger(ut));

        s.setValue("value2");
        s.update();

        assertTrue(ut.mState != null);

        assertTrue(storage.removeTrigger(ut));


        class DeleteTrigger extends Trigger<StorableVersioned> {
            Object mState;

            @Override
            public Object beforeDelete(StorableVersioned s) {
                assertEquals(1, s.getID());
                mState = new Object();
                return mState;
            }

            @Override
            public void afterDelete(StorableVersioned s, Object state) {
                assertEquals(1, s.getID());
                assertEquals(mState, state);
            }
        };

        DeleteTrigger dt = new DeleteTrigger();

        assertTrue(storage.addTrigger(dt));

        s.delete();

        assertTrue(dt.mState != null);

        assertTrue(storage.removeTrigger(dt));
    }

    public void test_triggerFailure() throws Exception {
        Storage<StorableVersioned> storage = getRepository().storageFor(StorableVersioned.class);

        class InsertTrigger extends Trigger<StorableVersioned> {
            boolean failed;

            @Override
            public Object beforeInsert(StorableVersioned s) {
                throw new RuntimeException();
            }

            @Override
            public void afterInsert(StorableVersioned s, Object state) {
                fail();
            }

            @Override
            public void failedInsert(StorableVersioned s, Object state) {
                failed = true;
            }
        };

        InsertTrigger it = new InsertTrigger();

        assertTrue(storage.addTrigger(it));

        StorableVersioned s = storage.prepare();
        s.setID(1);
        s.setValue("value");
        try {
            s.insert();
            fail();
        } catch (RuntimeException e) {
        }

        assertTrue(it.failed);

        class UpdateTrigger extends Trigger<StorableVersioned> {
            boolean failed;

            @Override
            public Object beforeUpdate(StorableVersioned s) {
                throw new RuntimeException();
            }

            @Override
            public void afterUpdate(StorableVersioned s, Object state) {
                fail();
            }

            @Override
            public void failedUpdate(StorableVersioned s, Object state) {
                failed = true;
            }
        };

        UpdateTrigger ut = new UpdateTrigger();

        assertTrue(storage.addTrigger(ut));

        s = storage.prepare();
        s.setID(1);
        s.setVersion(3);
        s.setValue("value");
        try {
            s.update();
            fail();
        } catch (RuntimeException e) {
        }

        assertTrue(ut.failed);

        class DeleteTrigger extends Trigger<StorableVersioned> {
            boolean failed;

            @Override
            public Object beforeDelete(StorableVersioned s) {
                throw new RuntimeException();
            }

            @Override
            public void afterDelete(StorableVersioned s, Object state) {
                fail();
            }

            @Override
            public void failedDelete(StorableVersioned s, Object state) {
                failed = true;
            }
        };

        DeleteTrigger dt = new DeleteTrigger();

        assertTrue(storage.addTrigger(dt));

        s = storage.prepare();
        s.setID(1);
        try {
            s.delete();
            fail();
        } catch (RuntimeException e) {
        }

        assertTrue(dt.failed);
    }

    public void test_triggerChecks() throws Exception {
        Storage<StorableTimestamped> storage =
            getRepository().storageFor(StorableTimestamped.class);

        StorableTimestamped st = storage.prepare();
        st.setId(1);
        st.setValue("value");

        try {
            st.insert();
            fail();
        } catch (ConstraintException e) {
            // We forgot to set submitDate and modifyDate.
        }

        // Install trigger that sets the timestamp properties.

        storage.addTrigger(new Trigger<Timestamped>() {
            @Override
            public Object beforeInsert(Timestamped st) {
                DateTime now = new DateTime();
                st.setSubmitDateTime(now);
                st.setModifyDateTime(now);
                return null;
            }

            @Override
            public Object beforeUpdate(Timestamped st) {
                DateTime now = new DateTime();
                st.setModifyDateTime(now);
                return null;
            }
        });

        st.insert();

        assertNotNull(st.getSubmitDateTime());
        assertNotNull(st.getModifyDateTime());

        DateTime dt = st.getModifyDateTime();

        Thread.sleep(500);

        st.setValue("foo");
        st.update();

        assertTrue(st.getModifyDateTime().getMillis() >= dt.getMillis() + 450);
    }

    public void test_triggerLoadByAltKey() throws Exception {
        Storage<StorableTestBasicCompoundIndexed> storage =
            getRepository().storageFor(StorableTestBasicCompoundIndexed.class);

        class LoadTrigger extends Trigger<StorableTestBasicCompoundIndexed> {
            int mCallCount;
            StorableTestBasicCompoundIndexed mStorable;

            @Override
            public void afterLoad(StorableTestBasicCompoundIndexed s) {
                mCallCount++;
                mStorable = s;
            }
        };

        LoadTrigger lt = new LoadTrigger();

        assertTrue(storage.addTrigger(lt));

        StorableTestBasicCompoundIndexed s = storage.prepare();
        s.initPropertiesRandomly(1234);
        s.insert();

        StorableTestBasicCompoundIndexed s2 = storage.prepare();
        s2.setStringProp(s.getStringProp());
        s2.setDoubleProp(s.getDoubleProp());
        s2.load();

        assertEquals(1, lt.mCallCount);
        assertTrue(s2 == lt.mStorable);
    }

    public void test_triggerUpdateNoLoad() throws Exception {
        // Verify that calling update does not call load trigger. This test is
        // needed because master storable generator does an intermediate load
        // in order to support "update full" mode.

        Storage<StorableVersioned> storage = getRepository().storageFor(StorableVersioned.class);

        class TheTrigger extends Trigger<StorableVersioned> {
            int mUpdateCount;
            int mLoadCount;

            @Override
            public Object beforeUpdate(StorableVersioned s) {
                mUpdateCount++;
                return null;
            }

            @Override
            public void afterLoad(StorableVersioned s) {
                mLoadCount++;
            }
        };

        TheTrigger the = new TheTrigger();

        assertTrue(storage.addTrigger(the));

        StorableVersioned s = storage.prepare();
        s.setID(1);
        s.setValue("value");
        s.insert();

        s.setValue("foo");
        s.update();

        assertEquals(1, the.mUpdateCount);
        assertEquals(0, the.mLoadCount);

        s.load();

        assertEquals(1, the.mUpdateCount);
        assertEquals(1, the.mLoadCount);
    }

    public void test_hashCode() throws Exception {
        Storage<StorableTestBasic> storage =
            getRepository().storageFor(StorableTestBasic.class);

        StorableTestBasic stb = storage.prepare();
        // Just tests to make sure generated code doesn't throw an error.
        int hashCode = stb.hashCode();
    }

    public void test_stateMethods() throws Exception {
        Storage<Order> storage = getRepository().storageFor(Order.class);

        Order order = storage.prepare();

        assertUninitialized(true, order, "orderID", "orderNumber", "orderTotal", "orderComments");
        assertDirty(false, order, "orderID", "orderNumber", "orderTotal", "orderComments");
        assertClean(false, order, "orderID", "orderNumber", "orderTotal", "orderComments");

        order.setOrderID(1);

        assertUninitialized(false, order, "orderID");
        assertUninitialized(true, order, "orderNumber", "orderTotal", "orderComments");
        assertDirty(false, order, "orderNumber", "orderTotal", "orderComments");
        assertDirty(true, order, "orderID");
        assertClean(false, order, "orderID", "orderNumber", "orderTotal", "orderComments");

        order.setOrderNumber("123");
        order.setOrderTotal(456);
        order.setAddressID(789);

        assertUninitialized(false, order, "orderID", "orderNumber", "orderTotal");
        assertUninitialized(true, order, "orderComments");
        assertDirty(true, order, "orderID", "orderNumber", "orderTotal");
        assertDirty(false, order, "orderComments");
        assertClean(false, order, "orderID", "orderNumber", "orderTotal", "orderComments");

        // Get unknown property
        try {
            order.isPropertyUninitialized("foo");
            fail();
        } catch (IllegalArgumentException e) {
        }

        // Get unknown property
        try {
            order.isPropertyDirty("foo");
            fail();
        } catch (IllegalArgumentException e) {
        }

        // Get unknown property
        try {
            order.isPropertyClean("foo");
            fail();
        } catch (IllegalArgumentException e) {
        }

        // Get join property
        try {
            order.isPropertyUninitialized("address");
            fail();
        } catch (IllegalArgumentException e) {
        }

        // Get join property
        try {
            order.isPropertyDirty("address");
            fail();
        } catch (IllegalArgumentException e) {
        }

        // Get join property
        try {
            order.isPropertyClean("address");
            fail();
        } catch (IllegalArgumentException e) {
        }

        order.insert();

        assertUninitialized(false, order, "orderID", "orderNumber", "orderTotal", "orderComments");
        assertDirty(false, order, "orderID", "orderNumber", "orderTotal", "orderComments");
        assertClean(true, order, "orderID", "orderNumber", "orderTotal", "orderComments");
    }

    public void test_count() throws Exception {
        test_count(false);
    }
    public void test_countIndexed() throws Exception {
        test_count(true);
    }

    private void test_count(boolean indexed) throws Exception {
        Storage<? extends StorableTestBasic> storage;
        if (indexed) {
            storage = getRepository().storageFor(StorableTestBasicIndexed.class);
        } else {
            storage = getRepository().storageFor(StorableTestBasic.class);
        }

        assertEquals(0, storage.query().count());

        StorableTestBasic sb = storage.prepare();
        sb.setId(1);
        sb.setIntProp(1);
        sb.setLongProp(1);
        sb.setDoubleProp(1.1);
        sb.setStringProp("one");
        sb.insert();

        assertEquals(1, storage.query().count());
        assertEquals(0, storage.query().not().count());
        assertEquals(1, storage.query("stringProp = ?").with("one").count());
        assertEquals(0, storage.query("stringProp = ?").with("two").count());

        sb = storage.prepare();
        sb.setId(2);
        sb.setIntProp(2);
        sb.setLongProp(2);
        sb.setDoubleProp(2.1);
        sb.setStringProp("two");
        sb.insert();

        sb = storage.prepare();
        sb.setId(3);
        sb.setIntProp(3);
        sb.setLongProp(3);
        sb.setDoubleProp(3.1);
        sb.setStringProp("three");
        sb.insert();

        assertEquals(3, storage.query().count());
        assertEquals(0, storage.query().not().count());
        assertEquals(1, storage.query("stringProp = ?").with("one").count());
        assertEquals(1, storage.query("stringProp = ?").with("two").count());
        assertEquals(1, storage.query("stringProp = ?").with("three").count());
        assertEquals(2, storage.query("stringProp = ?").not().with("one").count());
        assertEquals(2, storage.query("stringProp = ?").not().with("two").count());
        assertEquals(2, storage.query("stringProp = ?").not().with("three").count());
        assertEquals(2, storage.query("stringProp > ?").with("one").count());
        assertEquals(0, storage.query("stringProp > ?").with("two").count());
        assertEquals(1, storage.query("stringProp > ?").with("three").count());
    }

    public void test_fetchAfter() throws Exception {
        Storage<ManyKeys2> storage = getRepository().storageFor(ManyKeys2.class);

        final int groupSize = 4;
        final int aliasing = groupSize + 1;

        int total = 0;
        for (int a=0; a<groupSize; a++) {
            for (int b=0; b<groupSize; b++) {
                for (int c=0; c<groupSize; c++) {
                    for (int d=0; d<groupSize; d++) {
                        for (int e=0; e<groupSize; e++) {
                            for (int f=0; f<groupSize; f++) {
                                ManyKeys2 obj = storage.prepare();
                                obj.setA(a);
                                obj.setB(b);
                                obj.setC(c);
                                obj.setD(d);
                                obj.setE(e);
                                obj.setF(f);
                                obj.insert();
                                total++;
                            }
                        }
                    }
                }
            }
        }

        String[] orderBy = {"a", "b", "c", "d", "e", "f"};
        Query<ManyKeys2> query = storage.query().orderBy(orderBy);
        Cursor<ManyKeys2> cursor = query.fetch();
        Comparator<ManyKeys2> comparator = SortedCursor.createComparator(ManyKeys2.class, orderBy);

        int actual = 0;
        ManyKeys2 last = null;
        while (cursor.hasNext()) {
            actual++;
            ManyKeys2 obj = cursor.next();
            if (last != null) {
                assertTrue(comparator.compare(last, obj) < 0);
            }
            if (actual % aliasing == 0) {
                cursor.close();
                cursor = query.fetchAfter(obj);
            }
            last = obj;
        }

        assertEquals(total, actual);

        // Try again in reverse

        orderBy = new String[] {"-a", "-b", "-c", "-d", "-e", "-f"};
        query = storage.query().orderBy(orderBy);
        cursor = query.fetch();

        actual = 0;
        last = null;
        while (cursor.hasNext()) {
            actual++;
            ManyKeys2 obj = cursor.next();
            if (last != null) {
                assertTrue(comparator.compare(last, obj) > 0);
            }
            if (actual % aliasing == 0) {
                cursor.close();
                cursor = query.fetchAfter(obj);
            }
            last = obj;
        }

        assertEquals(total, actual);

        // Try using "after" method and additional filtering.

        orderBy = new String[] {"a", "b", "c", "d", "e", "f"};
        query = storage.query().orderBy(orderBy);
        {
            ManyKeys2 obj = storage.prepare();
            obj.setA(0);
            obj.setB(1);
            obj.setC(2);
            query = query.after(obj);
        }
        query = query.and("d = ?").with(3);
        // FIXME: shows "& d = 0", although results are correct.
        System.out.println(query);
        System.out.println(query.getFilter());
        System.out.println(query.getFilterValues());

        cursor = query.fetch();

        actual = 0;
        last = null;
        while (cursor.hasNext()) {
            actual++;
            ManyKeys2 obj = cursor.next();
            if (last != null) {
                assertTrue(comparator.compare(last, obj) < 0);
            }
            last = obj;
        }

        assertEquals(928, actual);

        // Try not after.

        orderBy = new String[] {"a", "b", "c", "d", "e", "f"};
        query = storage.query().orderBy(orderBy);
        {
            ManyKeys2 obj = storage.prepare();
            obj.setA(0);
            obj.setB(1);
            obj.setC(2);
            query = query.after(obj);
        }
        long count = query.count();
        long ncount = query.not().count();

        assertEquals(total, count + ncount);

        // Try again with funny mix of orderings. This will likely cause sort
        // operations to be performed, thus making it very slow.

        orderBy = new String[] {"-a", "b", "-c", "d", "-e", "f"};
        query = storage.query().orderBy(orderBy);
        cursor = query.fetch();
        comparator = SortedCursor.createComparator(ManyKeys2.class, orderBy);

        actual = 0;
        last = null;
        while (cursor.hasNext()) {
            actual++;
            ManyKeys2 obj = cursor.next();
            if (last != null) {
                assertTrue(comparator.compare(last, obj) < 0);
            }
            if (actual % aliasing == 0) {
                cursor.close();
                cursor = query.fetchAfter(obj);
            }
            last = obj;
        }

        assertEquals(total, actual);
    }

    public void test_fetchSlice() throws Exception {
        Storage<StorableTestBasic> storage = getRepository().storageFor(StorableTestBasic.class);

        for (int i=0; i<100; i++) {
            StorableTestBasic sb = storage.prepare();
            sb.setId(i);
            sb.setIntProp(99 - i);
            sb.setLongProp(i);
            sb.setDoubleProp(i);
            sb.setStringProp(String.valueOf(i));
            sb.insert();
        }

        // No slice
        List<StorableTestBasic> results;
        results = storage.query().fetchSlice(0, null).toList();
        assertEquals(100, results.size());

        // To slice
        results = storage.query().fetchSlice(0, 50L).toList();
        assertEquals(50, results.size());
        results = storage.query().orderBy("id").fetchSlice(0, 50L).toList();
        assertEquals(50, results.size());
        assertEquals(0, results.get(0).getId());
        assertEquals(1, results.get(1).getId());
        results = storage.query().orderBy("intProp").fetchSlice(0, 50L).toList();
        assertEquals(50, results.size());
        assertEquals(99, results.get(0).getId());
        assertEquals(98, results.get(1).getId());

        // From slice
        results = storage.query().fetchSlice(40, null).toList();
        assertEquals(60, results.size());
        results = storage.query().orderBy("id").fetchSlice(40, null).toList();
        assertEquals(60, results.size());
        assertEquals(40, results.get(0).getId());
        assertEquals(41, results.get(1).getId());
        results = storage.query().orderBy("intProp").fetchSlice(40, null).toList();
        assertEquals(60, results.size());
        assertEquals(59, results.get(0).getId());
        assertEquals(58, results.get(1).getId());

        // From and to slice
        results = storage.query().fetchSlice(40, 50L).toList();
        assertEquals(10, results.size());
        results = storage.query().orderBy("id").fetchSlice(40, 50L).toList();
        assertEquals(10, results.size());
        assertEquals(40, results.get(0).getId());
        assertEquals(41, results.get(1).getId());
        results = storage.query().orderBy("intProp").fetchSlice(40, 50L).toList();
        assertEquals(10, results.size());
        assertEquals(59, results.get(0).getId());
        assertEquals(58, results.get(1).getId());

        // Filter and slice
        Query<StorableTestBasic> query = storage.query("doubleProp >= ?").with(30.0);
        results = query.fetchSlice(10, 20L).toList();
        assertEquals(10, results.size());
        results = query.orderBy("id").fetchSlice(10, 20L).toList();
        assertEquals(10, results.size());
        assertEquals(40, results.get(0).getId());
        assertEquals(41, results.get(1).getId());
        results = query.orderBy("intProp").fetchSlice(10, 20L).toList();
        assertEquals(10, results.size());
        assertEquals(89, results.get(0).getId());
        assertEquals(88, results.get(1).getId());
    }

    public void test_lobInsert() throws Exception {
        Storage<StorableWithLobs> storage = getRepository().storageFor(StorableWithLobs.class);

        // Test null insert
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.insert();
            assertEquals(null, lobs.getBlobValue());
            assertEquals(null, lobs.getClobValue());
            lobs.load();
            assertEquals(null, lobs.getBlobValue());
            assertEquals(null, lobs.getClobValue());
        }

        // Test content insert
        int id;
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world"));
            lobs.insert();
            assertEquals("hello", lobs.getBlobValue().asString());
            assertEquals("world", lobs.getClobValue().asString());
            lobs.load();
            assertEquals("hello", lobs.getBlobValue().asString());
            assertEquals("world", lobs.getClobValue().asString());
            id = lobs.getId();
        }

        // Test update of inserted lobs
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setId(id);
            lobs.load();

            Blob blob = lobs.getBlobValue();
            OutputStream out = blob.openOutputStream();
            out.write("the sky is falling".getBytes());
            out.close();

            assertEquals("the sky is falling", blob.asString());

            lobs.load();
            assertEquals(blob.asString(), lobs.getBlobValue().asString());

            Clob clob = lobs.getClobValue();
            Writer writer = clob.openWriter();
            writer.write("the quick brown fox");
            writer.close();

            assertEquals("the quick brown fox", clob.asString());

            lobs.load();
            assertEquals(blob.asString(), lobs.getBlobValue().asString());
            assertEquals(clob.asString(), lobs.getClobValue().asString());
        }

        // Test insert failure
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setId(id);

            Blob newBlob = new ByteArrayBlob("blob insert should fail".getBytes());
            Clob newClob = new StringClob("clob insert should fail");

            lobs.setBlobValue(newBlob);
            lobs.setClobValue(newClob);

            try {
                lobs.insert();
                fail();
            } catch (UniqueConstraintException e) {
            }

            assertTrue(newBlob == lobs.getBlobValue());
            assertTrue(newClob == lobs.getClobValue());
        }
    }

    public void test_lobUpdate() throws Exception {
        Storage<StorableWithLobs> storage = getRepository().storageFor(StorableWithLobs.class);

        // Test null replaces null
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.insert();

            lobs.setBlobValue(null);
            lobs.setClobValue(null);

            lobs.update();

            assertEquals(null, lobs.getBlobValue());
            assertEquals(null, lobs.getClobValue());

            lobs.load();
            assertEquals(null, lobs.getBlobValue());
            assertEquals(null, lobs.getClobValue());
        }

        // Test null replaces content and verify content deleted
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world!!!"));
            lobs.insert();

            Blob blob = lobs.getBlobValue();
            Clob clob = lobs.getClobValue();

            assertEquals(5, blob.getLength());
            assertEquals(8, clob.getLength());

            lobs.setBlobValue(null);

            lobs.update();

            assertNull(lobs.getBlobValue());
            assertEquals(clob.asString(), lobs.getClobValue().asString());

            try {
                blob.getLength();
                fail();
            } catch (FetchNoneException e) {
            }
            assertEquals(8, clob.getLength());

            lobs.load();

            assertNull(lobs.getBlobValue());
            assertEquals(clob.asString(), lobs.getClobValue().asString());

            lobs.setClobValue(null);

            lobs.update();

            assertNull(lobs.getBlobValue());
            assertNull(lobs.getClobValue());

            try {
                blob.getLength();
                fail();
            } catch (FetchNoneException e) {
            }
            try {
                clob.getLength();
                fail();
            } catch (FetchNoneException e) {
            }

            lobs.load();

            assertNull(lobs.getBlobValue());
            assertNull(lobs.getClobValue());

            try {
                blob.setLength(100);
                fail();
            } catch (PersistNoneException e) {
            }
            try {
                clob.setLength(100);
                fail();
            } catch (PersistNoneException e) {
            }

            try {
                blob.setValue("hello");
                fail();
            } catch (PersistNoneException e) {
            }
            try {
                clob.setValue("hello");
                fail();
            } catch (PersistNoneException e) {
            }
        }

        // Test content replaces null
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.insert();

            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world"));

            assertTrue(lobs.getBlobValue() instanceof ByteArrayBlob);
            assertTrue(lobs.getClobValue() instanceof StringClob);

            lobs.update();

            assertEquals("hello", lobs.getBlobValue().asString());
            assertEquals("world", lobs.getClobValue().asString());

            assertFalse(lobs.getBlobValue() instanceof ByteArrayBlob);
            assertFalse(lobs.getClobValue() instanceof StringClob);

            lobs.load();

            assertEquals("hello", lobs.getBlobValue().asString());
            assertEquals("world", lobs.getClobValue().asString());

            assertFalse(lobs.getBlobValue() instanceof ByteArrayBlob);
            assertFalse(lobs.getClobValue() instanceof StringClob);
        }

        // Test content replaces content of same length
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world?"));
            lobs.insert();

            Blob blob = lobs.getBlobValue();
            Clob clob = lobs.getClobValue();

            lobs.setBlobValue(new ByteArrayBlob("12345".getBytes()));
            lobs.update();

            assertEquals(5, lobs.getBlobValue().getLength());
            assertEquals(6, lobs.getClobValue().getLength());

            assertEquals("12345", lobs.getBlobValue().asString());
            assertEquals("world?", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));

            lobs.setClobValue(new StringClob("123456"));
            lobs.update();

            assertEquals(5, lobs.getBlobValue().getLength());
            assertEquals(6, lobs.getClobValue().getLength());

            assertEquals("12345", lobs.getBlobValue().asString());
            assertEquals("123456", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));
        }

        // Test content replaces content of longer length
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world?"));
            lobs.insert();

            Blob blob = lobs.getBlobValue();
            Clob clob = lobs.getClobValue();

            lobs.setBlobValue(new ByteArrayBlob("123".getBytes()));
            lobs.update();

            assertEquals(3, lobs.getBlobValue().getLength());
            assertEquals(6, lobs.getClobValue().getLength());

            assertEquals("123", lobs.getBlobValue().asString());
            assertEquals("world?", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));

            lobs.setClobValue(new StringClob("12"));
            lobs.update();

            assertEquals(3, lobs.getBlobValue().getLength());
            assertEquals(2, lobs.getClobValue().getLength());

            assertEquals("123", lobs.getBlobValue().asString());
            assertEquals("12", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));
        }

        // Test content replaces content of shorter length
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world?"));
            lobs.insert();

            Blob blob = lobs.getBlobValue();
            Clob clob = lobs.getClobValue();

            lobs.setBlobValue(new ByteArrayBlob("123456789".getBytes()));
            lobs.update();

            assertEquals(9, lobs.getBlobValue().getLength());
            assertEquals(6, lobs.getClobValue().getLength());

            assertEquals("123456789", lobs.getBlobValue().asString());
            assertEquals("world?", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));

            lobs.setClobValue(new StringClob("1234567890"));
            lobs.update();

            assertEquals(9, lobs.getBlobValue().getLength());
            assertEquals(10, lobs.getClobValue().getLength());

            assertEquals("123456789", lobs.getBlobValue().asString());
            assertEquals("1234567890", lobs.getClobValue().asString());

            assertTrue(blob.asString().equals(lobs.getBlobValue().asString()));
            assertTrue(clob.asString().equals(lobs.getClobValue().asString()));
        }

        // Test update failure
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setId(10000);

            Blob newBlob = new ByteArrayBlob("blob update should fail".getBytes());
            Clob newClob = new StringClob("clob update should fail");

            lobs.setBlobValue(newBlob);
            lobs.setClobValue(newClob);

            try {
                lobs.update();
                fail();
            } catch (PersistNoneException e) {
            }

            assertTrue(newBlob == lobs.getBlobValue());
            assertTrue(newClob == lobs.getClobValue());
        }
    }

    public void test_lobDelete() throws Exception {
        Storage<StorableWithLobs> storage = getRepository().storageFor(StorableWithLobs.class);

        // Test delete of null lob
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.insert();
            lobs.delete();
            assertEquals(null, lobs.getBlobValue());
            assertEquals(null, lobs.getClobValue());
        }

        // Test delete of non-null lob
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setBlobValue(new ByteArrayBlob("hello".getBytes()));
            lobs.setClobValue(new StringClob("world?"));
            lobs.insert();

            Blob blob = lobs.getBlobValue();
            Clob clob = lobs.getClobValue();

            lobs.delete();

            try {
                blob.getLength();
                fail();
            } catch (FetchNoneException e) {
            }

            try {
                clob.getLength();
                fail();
            } catch (FetchNoneException e) {
            }

            try {
                blob.setLength(100);
                fail();
            } catch (PersistNoneException e) {
            }
            try {
                clob.setLength(100);
                fail();
            } catch (PersistNoneException e) {
            }

            try {
                blob.setValue("hello");
                fail();
            } catch (PersistNoneException e) {
            }
            try {
                clob.setValue("hello");
                fail();
            } catch (PersistNoneException e) {
            }
        }

        // Test delete failure
        {
            StorableWithLobs lobs = storage.prepare();
            lobs.setId(10000);

            Blob newBlob = new ByteArrayBlob("blob update should fail".getBytes());
            Clob newClob = new StringClob("clob update should fail");

            lobs.setBlobValue(newBlob);
            lobs.setClobValue(newClob);

            try {
                lobs.delete();
                fail();
            } catch (PersistNoneException e) {
            }

            assertTrue(newBlob == lobs.getBlobValue());
            assertTrue(newClob == lobs.getClobValue());
        }
    }

    public void test_insertLobBig() throws Exception {
        // LobEngine tests are fairly exhaustive when it comes to large
        // content. This is just a basic check.

        final long seed = 287623451234L;
        final int length = 123456;

        Storage<StorableWithLobs> storage = getRepository().storageFor(StorableWithLobs.class);

        StorableWithLobs lobs = storage.prepare();
        lobs.setBlobValue(new ByteArrayBlob(1));
        lobs.insert();

        Random rnd = new Random(seed);
        OutputStream out = lobs.getBlobValue().openOutputStream(0, 500);
        for (int i=0; i<length; i++) {
            out.write(rnd.nextInt());
        }
        out.close();

        assertEquals(length, lobs.getBlobValue().getLength());

        lobs.load();

        rnd = new Random(seed);
        InputStream in = lobs.getBlobValue().openInputStream(0, 2000);
        for (int i=0; i<length; i++) {
            assertEquals(rnd.nextInt() & 0xff, in.read());
        }
        assertEquals(-1, in.read());
        in.close();

        // Verify content stored in StoredLob.Block.
        // Only applicable if LobEngine is used.
        /*
        Query<StoredLob.Block> query = getRepository().storageFor(StoredLob.Block.class).query();
        Cursor<StoredLob.Block> cursor = query.fetch();
        assertTrue(cursor.hasNext());
        cursor.close();

        // Verify its all gone after delete.
        lobs.delete();

        cursor = query.fetch();
        assertFalse(cursor.hasNext());
        cursor.close();

        // Master record should be gone too.
        Cursor<?> c = getRepository().storageFor(StoredLob.class).query().fetch();
        assertFalse(c.hasNext());
        c.close();
        */
    }

    private void assertUninitialized(boolean expected, Storable storable, String... properties) {
        for (String property : properties) {
            assertEquals(expected, storable.isPropertyUninitialized(property));
        }
    }

    private void assertDirty(boolean expected, Storable storable, String... properties) {
        for (String property : properties) {
            assertEquals(expected, storable.isPropertyDirty(property));
        }
    }

    private void assertClean(boolean expected, Storable storable, String... properties) {
        for (String property : properties) {
            assertEquals(expected, storable.isPropertyClean(property));
        }
    }

    @PrimaryKey("id")
    public interface StorableTestAssymetricGet extends Storable{
        public abstract int getId();
        public abstract void setId(int id);

        public abstract int getAssymetricGET();
    }

    @PrimaryKey("id")
    public interface StorableTestAssymetricSet extends Storable{
        public abstract int getId();
        public abstract void setId(int id);

        public abstract int setAssymetricSET();
    }

    @PrimaryKey("id")
    public interface StorableTestAssymetricGetSet extends Storable{
        public abstract int getId();
        public abstract void setId(int id);

        public abstract int getAssymetricGET();

        public abstract int setAssymetricSET();
    }

    public static class InvocationTracker extends StorableTestBasic {
        String mName;
        long mInvocationTracks;

        boolean mTrace;

        public InvocationTracker(String name) {
            this(name, false);
        }

        public InvocationTracker(final String name, boolean trace) {
            mName = name;
            mTrace = trace;
            clearTracks();
        }

        public void clearTracks() {
            mInvocationTracks = 0;
        }

        public long getTracks() {
            return mInvocationTracks;
        }

        public void assertTrack(long value) {
            assertEquals(value, getTracks());
            clearTracks();
        }


        public void setId(int id) {
            if (mTrace) System.out.println("setId");
            mInvocationTracks |= sSetId;
        }

        // Basic coverage of the primitives
        public String getStringProp() {
            if (mTrace) System.out.println("getStringProp");
            mInvocationTracks |= sGetStringProp;  // 0x2
            return null;
        }

        public void setStringProp(String aStringThing) {
            if (mTrace) System.out.println("setStringProp");
            mInvocationTracks |= sSetStringProp;  // 0x4
        }

        public int getIntProp() {
            if (mTrace) System.out.println("getIntProp");
            mInvocationTracks |= sGetIntProp; // 0x8
            return 0;
        }

        public void setIntProp(int anInt) {
            if (mTrace) System.out.println("setIntProp");
            mInvocationTracks |= sSetIntProp; // 0x10
        }

        public long getLongProp() {
            if (mTrace) System.out.println("getLongProp");
            mInvocationTracks |= sGetLongProp;  // 0x20
            return 0;
        }

        public void setLongProp(long aLong) {
            if (mTrace) System.out.println("setLongProp");
            mInvocationTracks |= sSetLongProp;   // 0x40
        }

        public double getDoubleProp() {
            if (mTrace) System.out.println("getDoubleProp");
            mInvocationTracks |= sGetDoubleProp;   // 0x80
            return 0;
        }

        public void setDoubleProp(double aDouble) {
            if (mTrace) System.out.println("setDoubleProp");
            mInvocationTracks |= sSetDoubleProp;   // 0x100
        }

        public DateTime getDate() {
            return null;
        }

        public void setDate(DateTime date) {
        }

        public void load() throws FetchException {
            if (mTrace) System.out.println("load");
            mInvocationTracks |= sLoad;  // 0x200
        }

        public boolean tryLoad() throws FetchException {
            if (mTrace) System.out.println("tryLoad");
            mInvocationTracks |= sTryLoad;  // 0x400
            return false;
        }

        public void insert() throws PersistException {
            if (mTrace) System.out.println("insert");
            mInvocationTracks |= sInsert;  // 0x800
        }

        public boolean tryInsert() throws PersistException {
            if (mTrace) System.out.println("tryInsert");
            mInvocationTracks |= sTryInsert;
            return false;
        }

        public void update() throws PersistException {
            if (mTrace) System.out.println("update");
            mInvocationTracks |= sUpdate;
        }

        public boolean tryUpdate() throws PersistException {
            if (mTrace) System.out.println("tryUpdate");
            mInvocationTracks |= sTryUpdate;    // 0x1000
            return false;
        }

        public void delete() throws PersistException {
            if (mTrace) System.out.println("delete");
            mInvocationTracks |= sDelete;
        }

        public boolean tryDelete() throws PersistException {
            if (mTrace) System.out.println("tryDelete");
            mInvocationTracks |= sTryDelete;    // 0x2000
            return false;
        }

        public Storage storage() {
            if (mTrace) System.out.println("storage");
            mInvocationTracks |= sStorage;    // 0x4000
            return null;
        }

        public Storable copy() {
            if (mTrace) System.out.println("copy");
            mInvocationTracks |= sCopy;    // 0x8000
            return null;
        }

        public Storable prepare() {
            if (mTrace) System.out.println("prepare");
            return null;
        }

        public String toStringKeyOnly() {
            if (mTrace) System.out.println("toStringKeyOnly");
            mInvocationTracks |= sToStringKeyOnly;    // 0x10000
            return null;
        }

        public int getId() {
            if (mTrace) System.out.println("getId");
            mInvocationTracks |= sGetId;   // 0x20000
            return 0;
        }

        public void copyAllProperties(Storable storable) {
            if (mTrace) System.out.println("copyAllProperties");
            mInvocationTracks |= sCopyAllProperties;   // 0x40000
        }

        public void copyPrimaryKeyProperties(Storable storable) {
            if (mTrace) System.out.println("copyPrimaryKeyProperties");
            mInvocationTracks |= sCopyPrimaryKeyProperties;   // 0x80000
        }

        public void copyUnequalProperties(Storable storable) {
            if (mTrace) System.out.println("copyUnequalProperties");
            mInvocationTracks |= sCopyUnequalProperties;   // 0x10 0000
        }

        public void copyDirtyProperties(Storable storable) {
            if (mTrace) System.out.println("copyDirtyProperties");
            mInvocationTracks |= sCopyDirtyProperties;   // 0x20 0000
        }

        public boolean hasDirtyProperties() {
            if (mTrace) System.out.println("hasDirtyProperties");
            mInvocationTracks |= sHasDirtyProperties;   // 0x200 0000
            return false;
        }

        public boolean equalPrimaryKeys(Object obj) {
            if (mTrace) System.out.println("equalPrimaryKeys");
            mInvocationTracks |= sEqualKeys;   // 0x40 0000
            return true;
        }

        public boolean equalProperties(Object obj) {
            if (mTrace) System.out.println("equalProperties");
            mInvocationTracks |= sEqualProperties;   // 0x80 0000
            return true;
        }

        public void copyVersionProperty(Storable storable) {
            if (mTrace) System.out.println("copyVersionProperty");
            mInvocationTracks |= sCopyVersionProperty;   // 0x100 0000
        }

        public void markPropertiesClean() {
            if (mTrace) System.out.println("markPropertiesClean");
            mInvocationTracks |= sMarkPropertiesClean;   // 0x400 0000
        }

        public void markAllPropertiesClean() {
            if (mTrace) System.out.println("markAllPropertiesClean");
            mInvocationTracks |= sMarkAllPropertiesClean;   // 0x800 0000
        }

        public void markPropertiesDirty() {
            if (mTrace) System.out.println("markPropertiesDirty");
            mInvocationTracks |= sMarkPropertiesDirty;   // 0x1000 0000
        }

        public void markAllPropertiesDirty() {
            if (mTrace) System.out.println("markAllPropertiesDirty");
            mInvocationTracks |= sMarkAllPropertiesDirty;   // 0x2000 0000
        }

        public Class storableType() {
            if (mTrace) System.out.println("storableType");
            mInvocationTracks |= sStorableType;   // 0x4000 0000
            return Storable.class;
        }

        public boolean isPropertyUninitialized(String name) {
            if (mTrace) System.out.println("isPropertyUninitialized");
            return false;
        }

        public boolean isPropertyDirty(String name) {
            if (mTrace) System.out.println("isPropertyDirty");
            return false;
        }

        public boolean isPropertyClean(String name) {
            if (mTrace) System.out.println("isPropertyClean");
            return false;
        }

        public boolean isPropertySupported(String name) {
            if (mTrace) System.out.println("isPropertySupported");
            return false;
        }

        public Object getPropertyValue(String name) {
            if (mTrace) System.out.println("getPropertyValue(" + name + ')');
            return null;
        }

        public void setPropertyValue(String name, Object value) {
            if (mTrace) System.out.println("setPropertyValue(" + name + ", " + value + ')');
        }

        public Map<String, Object> propertyMap() {
            throw new UnsupportedOperationException();
        }

        public void writeTo(OutputStream out) {
            throw new UnsupportedOperationException();
        }

        public void readFrom(InputStream in) {
            throw new UnsupportedOperationException();
        }
    }

}
