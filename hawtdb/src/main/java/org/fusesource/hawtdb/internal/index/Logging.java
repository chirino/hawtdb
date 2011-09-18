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
package org.fusesource.hawtdb.internal.index;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Logging {

    private static final Log LOG = LogFactory.getLog(Logging.class);

    public static void trace(String message, Object...args) {
        if( LOG.isTraceEnabled() ) {
            LOG.trace(String.format(message, args));
        }
    }
    public static void debug(String message, Object...args) {
        if( LOG.isDebugEnabled() ) {
            LOG.debug(String.format(message, args));
        }
    }

    public static void warn(String message, Object...args) {
        if( LOG.isWarnEnabled() ) {
            LOG.warn(String.format(message, args));
        }
    }
    public static void warn(Throwable e, String message, Object...args) {
        if( LOG.isWarnEnabled() ) {
            LOG.warn(String.format(message, args), e);
        }
    }

    public static void error(String message, Object...args) {
        if( LOG.isErrorEnabled() ) {
            LOG.error(String.format(message, args));
        }
    }
}