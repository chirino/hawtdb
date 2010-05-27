/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fusesource.hawtdb.internal.journal;

import org.fusesource.hawtbuf.codec.Codec;
import org.fusesource.hawtbuf.codec.VarIntegerCodec;
import org.fusesource.hawtdb.internal.journal.Location;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implementation of a Marshaller for Location objects.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 *
 */
public class LocationCodec implements Codec<Location> {

    public static final LocationCodec INSTANCE = new LocationCodec();

    public void encode(Location object, DataOutput dataOut) throws IOException {
        VarIntegerCodec.INSTANCE.encode(object.getDataFileId(), dataOut);
        VarIntegerCodec.INSTANCE.encode(object.getOffset(), dataOut);
    }

    public Location decode(DataInput dataIn) throws IOException {
        int fileId = VarIntegerCodec.INSTANCE.decode(dataIn);
        int offset = VarIntegerCodec.INSTANCE.decode(dataIn);
        return new Location(fileId, offset);
    }

    public int getFixedSize() {
        return -1;
    }

    public Location deepCopy(Location source) {
        return new Location(source);
    }

    public boolean isDeepCopySupported() {
        return true;
    }

    public int estimatedSize(Location object) {
        return VarIntegerCodec.INSTANCE.estimatedSize(object.getDataFileId()) +
               VarIntegerCodec.INSTANCE.estimatedSize(object.getOffset());
    }
}
