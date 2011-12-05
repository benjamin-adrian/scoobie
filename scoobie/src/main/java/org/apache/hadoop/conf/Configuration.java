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

package org.apache.hadoop.conf;

import de.dfki.km.perspecting.obie.transducer.LanguageIdentification;

/**
 * 
 * This is a mock and a hack. It is the only class needed from the apache hadoop library.
 * It is used by {@link LanguageIdentification}.
 * 
 * @author adrian
 *
 */
public class Configuration {

	public int getInt(String name, int defaultValue) {
		return defaultValue;
	}

}
