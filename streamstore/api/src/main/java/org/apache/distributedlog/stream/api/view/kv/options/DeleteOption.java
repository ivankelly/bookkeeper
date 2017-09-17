/*
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

package org.apache.distributedlog.stream.api.view.kv.options;

import io.netty.buffer.ByteBuf;
import java.util.Optional;
import org.inferred.freebuilder.FreeBuilder;

/**
 * Delete Option.
 */
@FreeBuilder
public interface DeleteOption {

  Optional<ByteBuf> endKey();

  boolean prevKv();

  /**
   * Builder to build delete option.
   */
  class Builder extends DeleteOption_Builder {

    private Builder() {
      prevKv(false);
    }

  }

  static Builder newBuilder() {
    return new Builder();
  }

}
