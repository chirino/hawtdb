/**
 *  Copyright (C) 2009, Progress Software Corporation and/or its
 * subsidiaries or affiliates.  All rights reserved.
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
package org.fusesource.hawtdb.internal.page;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Logging {

    private static final HashSet<Integer> TRACED_PAGES=new HashSet<Integer>();
    static {
//        TRACED_PAGES.add(9);
//        TRACED_PAGES.add(12);
    }

    private static final Log LOG = LogFactory.getLog(Logging.class);

    public static boolean traced(int page) {
        return LOG.isTraceEnabled() && TRACED_PAGES.contains(page);
    }

    public static void trace(String message, Object...args) {
        if( LOG.isTraceEnabled() ) {
            LOG.trace(String.format(message, args));
        }
    }

}
