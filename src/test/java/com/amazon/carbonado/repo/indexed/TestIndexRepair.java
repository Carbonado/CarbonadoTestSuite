/*
 * Copyright 2008 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.indexed;

import java.util.*;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.amazon.carbonado.*;
import com.amazon.carbonado.repo.map.MapRepositoryBuilder;

import com.amazon.carbonado.stored.StorableTestBasicCompoundIndexed;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TestIndexRepair extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        return new TestSuite(TestIndexRepair.class);
    }

    private Repository mRepository;

    public TestIndexRepair(String name) {
        super(name);
    }

    public void test_shouldInsert() throws Exception {
        Repository repo = TestUtilities.buildTempRepository();
        test_shouldInsert(repo);
        repo.close();
    }

    public void test_shouldInsertMap() throws Exception {
        test_shouldInsert(MapRepositoryBuilder.newRepository());
    }

    private void test_shouldInsert(Repository repo) throws Exception {
        Storage<StorableTestBasicCompoundIndexed> storage =
            repo.storageFor(StorableTestBasicCompoundIndexed.class);

        final long correctCount = insertRecords(storage);

        IndexEntryAccessCapability cap = 
            repo.getCapability(IndexEntryAccessCapability.class);

        Random rnd = new Random(45634634);
        List<Storable> toDelete = new ArrayList<Storable>();

        for (IndexEntryAccessor<StorableTestBasicCompoundIndexed> acc
                 : cap.getIndexEntryAccessors(StorableTestBasicCompoundIndexed.class)) {
            Cursor<? extends Storable> entries = acc.getIndexEntryStorage().query().fetch();
            while (entries.hasNext()) {
                Storable entry = entries.next();
                if (rnd.nextInt(100) == 0) {
                    toDelete.add(entry);
                }
            }
        }

        for (Storable entry : toDelete) {
            entry.delete();
        }

        final long[] shortCounts = indexCounts(storage);
        final long shortCount = sumCounts(shortCounts);

        assertTrue(shortCount < (correctCount * 4));
        assertEquals(correctCount * 4, shortCount + toDelete.size());

        for (IndexEntryAccessor<StorableTestBasicCompoundIndexed> acc
                 : cap.getIndexEntryAccessors(StorableTestBasicCompoundIndexed.class)) {
            acc.repair(1.0);
        }

        final long[] reCounts = indexCounts(storage);
        final long reCount = sumCounts(reCounts);

        assertEquals(correctCount * 4, reCount);
    }

    public void test_shouldDelete() throws Exception {
        Repository repo = TestUtilities.buildTempRepository();
        test_shouldDelete(repo);
        repo.close();
    }

    public void test_shouldDeleteMap() throws Exception {
        test_shouldDelete(MapRepositoryBuilder.newRepository());
    }

    private void test_shouldDelete(Repository repo) throws Exception {
        Storage<StorableTestBasicCompoundIndexed> storage =
            repo.storageFor(StorableTestBasicCompoundIndexed.class);

        final long correctCount = insertRecords(storage);

        IndexEntryAccessCapability cap = 
            repo.getCapability(IndexEntryAccessCapability.class);

        Random rnd = new Random(245322638);
        List<StorableTestBasicCompoundIndexed> toDelete =
            new ArrayList<StorableTestBasicCompoundIndexed>();
        List<Storable> toAdd = new ArrayList<Storable>();

        for (IndexEntryAccessor<StorableTestBasicCompoundIndexed> acc
                 : cap.getIndexEntryAccessors(StorableTestBasicCompoundIndexed.class)) {
            Cursor<? extends Storable> entries = acc.getIndexEntryStorage().query().fetch();
            while (entries.hasNext()) {
                Storable entry = entries.next();
                if (rnd.nextInt(100) == 0) {
                    StorableTestBasicCompoundIndexed master = storage.prepare();
                    acc.copyToMasterPrimaryKey(entry, master);
                    toDelete.add(master);
                    toAdd.add(entry);
                }
            }
        }

        long newCount = correctCount;

        for (StorableTestBasicCompoundIndexed master : toDelete) {
            if (master.tryDelete()) {
                newCount--;
            }
        }

        for (Storable entry : toAdd) {
            entry.insert();
        }

        final long[] tallCounts = indexCounts(storage);
        final long tallCount = sumCounts(tallCounts);

        assertTrue(tallCount > newCount * 4);

        for (IndexEntryAccessor<StorableTestBasicCompoundIndexed> acc
                 : cap.getIndexEntryAccessors(StorableTestBasicCompoundIndexed.class)) {
            acc.repair(1.0);
        }
        
        final long[] reCounts = indexCounts(storage);
        final long reCount = sumCounts(reCounts);

        assertEquals(newCount * 4, reCount);
    }

    public void test_shouldUpdate() throws Exception {
        Repository repo = TestUtilities.buildTempRepository();
        test_shouldUpdate(repo);
        repo.close();
    }

    public void test_shouldUpdateMap() throws Exception {
        test_shouldUpdate(MapRepositoryBuilder.newRepository());
    }

    private void test_shouldUpdate(Repository repo) throws Exception {
        Storage<StorableTestBasicCompoundIndexed> storage =
            repo.storageFor(StorableTestBasicCompoundIndexed.class);

        final long correctCount = insertRecords(storage);

        IndexEntryAccessCapability cap = 
            repo.getCapability(IndexEntryAccessCapability.class);

        Random rnd = new Random(245322638);
        List<Storable> toUpdate = new ArrayList<Storable>();

        for (IndexEntryAccessor<StorableTestBasicCompoundIndexed> acc
                 : cap.getIndexEntryAccessors(StorableTestBasicCompoundIndexed.class)) {

            // Only muck with alternate key.
            String[] names = acc.getPropertyNames();
            if ("id".equals(names[names.length - 1])) {
                continue;
            }

            Cursor<? extends Storable> entries = acc.getIndexEntryStorage().query().fetch();
            while (entries.hasNext()) {
                Storable entry = entries.next();
                if (rnd.nextInt(100) == 0) {
                    toUpdate.add(entry);
                }
            }
        }

        // Make index entries point to master ids that won't match.
        for (Storable entry : toUpdate) {
            Integer id = (Integer) entry.getPropertyValue("id");
            entry.setPropertyValue("id", id + 100000000);
            entry.update();
        }

        final long[] counts = indexCounts(storage);
        final long count = sumCounts(counts);

        assertEquals(correctCount * 4, count);

        // Only way to tell entries are broken is by query, not count.
        List<StorableTestBasicCompoundIndexed> list =
            storage.query("stringProp >= ? & doubleProp >= ?").with("").with(0).fetch().toList();

        assertTrue(list.size() < correctCount);
        assertEquals(correctCount, list.size() + toUpdate.size());

        for (IndexEntryAccessor<StorableTestBasicCompoundIndexed> acc
                 : cap.getIndexEntryAccessors(StorableTestBasicCompoundIndexed.class)) {
            acc.repair(1.0);
        }

        final long[] reCounts = indexCounts(storage);
        final long reCount = sumCounts(reCounts);

        assertEquals(correctCount * 4, reCount);

        list =
            storage.query("stringProp >= ? & doubleProp >= ?").with("").with(0).fetch().toList();

        assertEquals(correctCount, list.size());
    }

    private long insertRecords(Storage<StorableTestBasicCompoundIndexed> storage)
        throws Exception
    {
        int count = 10000;
        for (int i=0; i<count; i++) {
            StorableTestBasicCompoundIndexed stb = storage.prepare();
            stb.setId(i);
            stb.setStringProp("str" + i);
            stb.setIntProp(i * 2);
            stb.setLongProp(i * 20L);
            stb.setDoubleProp(i * 1.5);
            stb.insert();
        }
        return count;
    }

    private long[] indexCounts(Storage<StorableTestBasicCompoundIndexed> storage)
        throws Exception
    {
        return new long[] {
            storage.query("stringProp >= ? & stringProp <= ? & intProp >= ?")
            .with("").with("xxx").with(0).count(),
            storage.query("intProp >= ?").with(0).count(),
            storage.query("doubleProp >= ?").with(0).count(),
            storage.query("stringProp >= ? & doubleProp >= ?").with("").with(0).count(),
        };
    }

    private long sumCounts(long[] counts) {
        long sum = 0;
        for (long count : counts) {
            sum += count;
        }
        return sum;
    }
}
