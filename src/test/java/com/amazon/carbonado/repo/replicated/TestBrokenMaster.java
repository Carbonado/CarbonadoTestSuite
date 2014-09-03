/*
 * Copyright 2006-2013 Amazon Technologies, Inc. or its affiliates.
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

import java.util.concurrent.atomic.AtomicReference;

import com.amazon.carbonado.ConfigurationException;
import com.amazon.carbonado.Repository;
import com.amazon.carbonado.RepositoryBuilder;
import com.amazon.carbonado.RepositoryException;
import com.amazon.carbonado.FetchException;
import com.amazon.carbonado.PersistException;
import com.amazon.carbonado.Storable;
import com.amazon.carbonado.Storage;
import com.amazon.carbonado.TestUtilities;
import com.amazon.carbonado.TriggerFactory;

import com.amazon.carbonado.layout.TestLayout;
import com.amazon.carbonado.stored.StorableTestMinimal;

import org.cojen.classfile.TypeDesc;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import static org.junit.Assert.fail;

/**
 * Test ReplicatedRepository when the master repository builder throws an
 * Exception at build time.
 *
 * @author Jesse Morgan (morganjm)
 */
public class TestBrokenMaster extends TestCase {
    private static final String STORABLE_NAME = "test.TestStorable";

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestBrokenMaster.class);
        return suite;
    }

    public TestBrokenMaster(String name) {
        super(name);
    }

    private RepositoryBuilder mReplicaBuilder;
    private BrokenRepositoryBuilder mMasterBuilder;
    private Repository mRepository;

    private Class<? extends StorableTestMinimal> mTestStorableClazz;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mReplicaBuilder = TestUtilities.newTempRepositoryBuilder("rr-replica", TestUtilities.DEFAULT_CAPACITY, false, false);
        mMasterBuilder = new BrokenRepositoryBuilder(TestUtilities.newTempRepositoryBuilder("rr-writer", TestUtilities.DEFAULT_CAPACITY, true, false));

        // Prepare the Storable class
        mTestStorableClazz = TestLayout.defineStorable(STORABLE_NAME, 0, TypeDesc.INT);

        // Populate data
        mRepository = buildRepository(false);
        Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(mTestStorableClazz);
        StorableTestMinimal s = storage.prepare();
        s.setId(1);
        s.insert();
        mRepository.close();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        if (mRepository != null) {
            mRepository.close();
            mRepository = null;
        }
    }

    private Repository buildRepository(boolean broken) throws RepositoryException {
        if (broken) {
            mMasterBuilder.setFailures(1);
        }
        ReplicatedRepositoryBuilder builder = new ReplicatedRepositoryBuilder();
        builder.setMaster(true);
        builder.setReplicaRepositoryBuilder(mReplicaBuilder);
        builder.setMasterRepositoryBuilder(mMasterBuilder);
        return builder.build();
    }

    /**
     * Verify that reads work and inserts fail on a broken ReplicatedRepository.
     */
    public void testLoadAndInsert() throws Exception {
        mRepository = buildRepository(true);
        Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(mTestStorableClazz);

        // Test loading existing an Storable with a broken master.
        // This should succeed.
        StorableTestMinimal s = storage.prepare();
        s.setId(1);
        s.load();

        // Test inserting a Storable with broken master.
        // This should fail.
        try {
            s = storage.prepare();
            s.setId(2);
            s.insert();

            fail("Expected exception.");
        } catch (PersistException e) {
            assertEquals("Current transaction is read-only.", e.getMessage());
        }
    }

    /**
     * Verify that Storables can't change on a broken ReplicatedRepository.
     */
    public void testStorableChanged() throws Exception {
        // Change the Storable definition
        mTestStorableClazz = TestLayout.defineStorable(STORABLE_NAME, 1, TypeDesc.INT);

        // Open the repository.
        mRepository = buildRepository(true);

        // Test loading existing an Storable with a broken master.
        // This should fail because the layout has changed and the new layout
        // can not be persisted.
        try {
            Storage<? extends StorableTestMinimal> storage = mRepository.storageFor(mTestStorableClazz);
            StorableTestMinimal s = storage.prepare();
            s.setId(1);
            s.load();
            fail("Expected exception");
        } catch (PersistException e) {
            assertEquals("com.amazon.carbonado.PersistException: Current transaction is read-only.", e.getMessage());
        }
    }

    /**
     * This class wraps a RepositoryBuilder and fails to build the first n times.
     *
     * This is used to imitate a JDBC timeout at startup.
     */
    class BrokenRepositoryBuilder implements RepositoryBuilder {
        private RepositoryBuilder mWrappedBuilder;
        private int mFailuresRemaining;

        public BrokenRepositoryBuilder(RepositoryBuilder builder) {
            mWrappedBuilder = builder;
        }

        public void setFailures(int i) {
            mFailuresRemaining = i;
        }

        @Override
        public Repository build() throws ConfigurationException, RepositoryException {
            return mWrappedBuilder.build();
        }

        @Override
        public Repository build(AtomicReference<Repository> rootReference)
            throws ConfigurationException, RepositoryException {

            if (mFailuresRemaining > 0) {
                mFailuresRemaining--;
                throw new FetchException("Failed Build Enabled");
            }

            return mWrappedBuilder.build(rootReference);
        }

        @Override
        public String getName() {
            return mWrappedBuilder.getName();
        }

        @Override
        public void setName(String name) {
            mWrappedBuilder.setName(name);
        }

        @Override
        public boolean isMaster() {
            return mWrappedBuilder.isMaster();
        }

        @Override
        public void setMaster(boolean b) {
            mWrappedBuilder.setMaster(b);
        }

        @Override
        public boolean addTriggerFactory(TriggerFactory factory) {
            return mWrappedBuilder.addTriggerFactory(factory);
        }

        @Override
        public boolean removeTriggerFactory(TriggerFactory factory) {
            return mWrappedBuilder.removeTriggerFactory(factory);
        }

        @Override
        public Iterable<TriggerFactory> getTriggerFactories() {
            return mWrappedBuilder.getTriggerFactories();
        }
    }
}
