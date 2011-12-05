/*
    Copyright (c) 2011, 
    Benjamin Adrian <benjamin.horak@gmail.com>
    German Research Center for Artificial Intelligence (DFKI) <info@dfki.de>
    
    All rights reserved.

    This file is part of SCOOBIE.

    SCOOBIE is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    SCOOBIE is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with SCOOBIE.  If not, see <http://www.gnu.org/licenses/>.
 */

package de.dfki.km.perspecting.obie.model;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import de.dfki.km.perspecting.obie.transducer.model.NounPhraseChunker;

@Ignore
public class TestNounPhraseChunker {
    private final static int WINDOW_SIZE = 3; // Window = +- WINDOW_SIZE

    private final static String TRAIN_FILE_EN = "/media/data/wikipedia2011/training_conll2000.txt";
    private final static String TRAIN_FILE_DE = "/media/data/scoobie/SCOOBIE/src/main/resources/de/dfki/km/perspecting/obie/connection/ontology/session/npc/de/training_tiger.txt";
    
    
    private final static String TEST_FILE = "/media/data/wikipedia2011/test_conll2000.txt";

    private final static String CRF_OUT_FILE = "/media/data/scoobie/SCOOBIE/src/main/resources/de/dfki/km/perspecting/obie/connection/ontology/session/npc/de/DE.crf";

    private static NounPhraseChunker npc = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        npc = new NounPhraseChunker(TRAIN_FILE_DE, TEST_FILE, CRF_OUT_FILE, WINDOW_SIZE);
        npc.init();
    }

    @Test
    public void testCrf() throws Exception {
        npc.test();
    }

    @Test
    public void trainCrf() throws Exception {
        npc.train();
    }
}
