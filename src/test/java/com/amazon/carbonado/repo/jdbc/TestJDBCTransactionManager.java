/*
 * Copyright 2008-2015 Amazon Technologies, Inc. or its affiliates.
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

package com.amazon.carbonado.repo.jdbc;

import com.amazon.carbonado.*;

import java.sql.Connection;
import java.sql.SQLException;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import static org.easymock.EasyMock.*;

/**
 * Tests for the JDBCTransactionManager.
 *
 * These tests mock out everything except the JDBCTransactionManager and
 * JDBCTransaction.
 *
 * @author Jesse Morgan
 */
public class TestJDBCTransactionManager extends TestCase {
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static TestSuite suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(TestJDBCTransactionManager.class);
        return suite;
    }

    private JDBCRepository mMockRepository;
    private Connection mMockConnection;

    /**
     * Default Test Constructor.
     */
    public TestJDBCTransactionManager(String name) {
        super(name);
    }

    /**
     * Setup Mocks for the test.
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        mMockConnection = createMock(Connection.class);

        mMockRepository = createMock(JDBCRepository.class);
        expect(mMockRepository.getConnectionForTxn(anyObject(IsolationLevel.class)))
            .andReturn(mMockConnection)
            .anyTimes();

        expect(mMockRepository.getExceptionTransformer())
            .andReturn(new JDBCExceptionTransformer())
            .anyTimes();
    }

    /**
     * Verify aborting a transaction rolls back and closes the connection.
     */
    public void testAbortTransaction() throws Exception {
        // Expected Results
        mMockConnection.rollback();
        mMockRepository.closeConnection(mMockConnection);
        replayAll();

        // Run Test
        JDBCTransactionManager txnManager = new JDBCTransactionManager(mMockRepository);
        JDBCTransaction t = txnManager.createTxn(null, IsolationLevel.READ_COMMITTED);
        txnManager.abortTxn(t);

        // Verify Results
        verifyAll();
    }

    /**
     * Test that aborting a transaction closes the connection, even if an exception is thrown.
     */
    public void testAbortTransactionWhenThrows() throws Exception {
        // Expected Results
        mMockConnection.rollback();
        expectLastCall().andThrow(new SQLException());
        mMockRepository.closeConnection(mMockConnection);
        replayAll();

        // Run Test
        JDBCTransactionManager txnManager = new JDBCTransactionManager(mMockRepository);
        JDBCTransaction t = txnManager.createTxn(null, IsolationLevel.READ_COMMITTED);

        try {
            txnManager.abortTxn(t);
            fail("Should have thrown.");
        } catch (PersistException e) {
            // Expected.
        }

        // Verify Results
        verifyAll();
    }

    /**
     * Test that aborting a transaction closes the connection, even if the repository is null.
     */
    public void testAbortTransactionWhenRepositoryClosed() throws Exception {
        // Expected Results
        mMockConnection.rollback();
        mMockConnection.close();
        replayAll();

        // Create a txn.
        JDBCTransactionManager txnManager = new JDBCTransactionManager(mMockRepository);
        JDBCTransaction t = txnManager.createTxn(null, IsolationLevel.READ_COMMITTED);

        // "Close" the repository.
        mMockRepository = null;
        System.gc();

        // Abort should still close the connection.
        txnManager.abortTxn(t);

        // Verify Results
        verify(mMockConnection);
    }

    /**
     * Prepare all the mocks to be replayed.
     */
    private void replayAll() {
        replay(mMockRepository, mMockConnection);
    }

    /**
     * Verify all the mocks.
     */
    private void verifyAll() {
        verify(mMockRepository, mMockConnection);
    }

}
