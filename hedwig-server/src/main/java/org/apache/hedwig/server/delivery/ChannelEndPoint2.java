/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.server.delivery;

import java.util.HashMap;
import java.util.Map;

import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.server.common.UnexpectedError;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelEndPoint2 implements DeliveryEndPoint,
		ChannelFutureListener {
	protected static final Logger logger = LoggerFactory
			.getLogger(ChannelEndPoint2.class);
	Channel channel;

	public Channel getChannel() {
		return channel;
	}

	Map<ChannelFuture, DeliveryCallback> callbacks = new HashMap<ChannelFuture, DeliveryCallback>();

	public ChannelEndPoint2(Channel channel) {
		this.channel = channel;
	}

	public void close() {
		channel.close();
	}

	public void send(PubSubResponse response, DeliveryCallback callback) {
		ChannelFuture future = channel.write(response);
		callbacks.put(future, callback);
		future.addListener(this);
		logger.info("[l...y] resend msg:"
				+ response.getMessage().getBody().toStringUtf8()
				+ " in channel................. ");

	}

	public void operationComplete(ChannelFuture future) throws Exception {
		DeliveryCallback callback = callbacks.get(future);
		callbacks.remove(future);

		if (callback == null) {
			throw new UnexpectedError(
					"Could not locate callback for channel future");
		}

		if (future.isSuccess()) {
			// just change here
			// don't want to change original code everywhere, so use the
			// following code

		} else {
			// treat all channel errors as permanent
			/* msgbus modified, add a parameter */

		}

	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ChannelEndPoint) {
			ChannelEndPoint channelEndPoint = (ChannelEndPoint) obj;
			return channel.equals(channelEndPoint.channel);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return channel.hashCode();
	}

	@Override
	public String toString() {
		return channel.toString();
	}
}
