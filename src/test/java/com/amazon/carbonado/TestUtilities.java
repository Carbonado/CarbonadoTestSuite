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

import java.io.File;
import java.io.IOException;
import java.util.Random;

import com.amazon.carbonado.repo.sleepycat.BDBRepositoryBuilder;

/**
 * TestUtilities
 *
 * @author Don Schneider
 * @author Brian S O'Neill
 */
public class TestUtilities {
    public static final String FILE_PATH_KEY = "filepath";

    // Keep memory usage low to prevent spurious out-of-memory errors while running tests.
    private static final int DEFAULT_CAPACITY = 100000;

    private static final Random sRandom = new Random();

    public static String makeTestDirectoryString(String nameElement) {
        return makeTestDirectory(nameElement).getAbsolutePath();
    }

    public static File makeTestDirectory(String nameElement) {
        String dirPath = System.getProperty(FILE_PATH_KEY);
        File dir;
        if (dirPath == null) {
            dir = new File(new File(System.getProperty("java.io.tmpdir")), "carbonado-test");
        } else {
            dir = new File(dirPath);
        }
        String path = nameElement + '-' + System.currentTimeMillis() + '-' + sRandom.nextInt(1000);
        File file = new File(dir, path);
        try {
            // For better error reporting
            file = file.getCanonicalFile();
        } catch (IOException e) {
            // Ignore.
        }
        // BDBRepositoryBuilder will make directory if it doesn't exist.
        return file;
    }

    public static Repository buildTempRepository() {
        return buildTempRepository("test");
    }

    public static Repository buildTempRepository(String name) {
        return buildTempRepository(name, DEFAULT_CAPACITY);
    }

    public static Repository buildTempRepository(String name, int capacity) {
        BDBRepositoryBuilder builder = new BDBRepositoryBuilder();
        builder.setProduct("JE");
        builder.setName(name);
        builder.setTransactionNoSync(true);
        builder.setCacheSize(capacity);
        builder.setLogInMemory(true);

        if (sTempRepoDir == null) {
            sTempRepoDir = makeTestDirectoryString(name);
        }

        builder.setEnvironmentHome(sTempRepoDir);

        try {
            return builder.build();
        } catch (RepositoryException e) {
            throw new UnsupportedOperationException("Could not create repository", e);
        }
    }

    private static String sTempRepoDir;

    private static String sAlphabet = "abcdefghijklmnopqrstuvwxyz";
    private static Random sNumbers = new Random();

    public static void sAddRandomWord(StringBuffer buffer, int aCount) {
        sAddRandomWord(buffer, aCount, sNumbers);
    }

    public static void sAddRandomWord(StringBuffer buffer, int aCount, Random rnd) {
        int position = rnd.nextInt(sAlphabet.length());
        char c = sAlphabet.charAt(position);
        for (int i = 0; i < aCount; i++) {
            buffer.append(c);
        }
    }

    public static String sRandomText(int aMin, int aMax) {
        return sRandomText(aMin, aMax, sNumbers);
    }

    public static String sRandomText(int aMin, int aMax, Random rnd) {
        return sRandomText(rnd.nextInt(aMax-aMin) + aMin, rnd);
    }

    public static String sRandomText(int aCount) {
        return sRandomText(aCount, sNumbers);
    }

    public static String sRandomText(int aCount, Random rnd) {
        if (aCount <= 0) {
            return "";
        }

        final StringBuffer buffer = new StringBuffer(aCount);
        int filled = 0;
        while (filled<aCount) {
            int wordsize = rnd.nextInt(Math.min(10, aCount - filled)) + 1;
            sAddRandomWord(buffer, wordsize, rnd);
            filled += wordsize;
            if (filled<aCount) {
                buffer.append(' ');
                filled++;
            }
        }
        return buffer.toString();
    }
}
