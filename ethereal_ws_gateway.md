# Websocket Gateway

## Overview

The gateway ws subscription allow users to listen to data streams in real-time data, delivering instant trading updates directly from the Ethereal exchange platform. To interact with the subscription gateway API, send websocket messages after connecting to `wss://ws.ethereal.trade/v1/stream`.

### Socket.IO

Ethereal uses [Socket.IO](https://socket.io/docs/v4/), a library that extends the standard WebSocket protocol. Socket.IO provides ready-made client libraries in multiple languages with automatic reconnection handling and built-in ping/pong frames to maintain stable connections, saving developers from implementing these basic features themselves.

Refer to [Socket.IO](https://socket.io/docs/v4/#client-implementations) for language specific client implementations.

{% hint style="warning" %}
Team is transitioning from SocketIO to native WebSockets for enhanced performance and improved interfaces will change. If you are using the Python SDK, the transition should be seamless aside from a future upgrade requirement.
{% endhint %}

## Subscription Streams

### Subscribe

The WebSocket gateway offers the following subscription streams:

**`BOOK_DEPTH`** - Provides order book depth updates for a specific product.

```json
// Subscription message payload
{
  "type": "BookDepth",
  "productId": "<uuid>"
}

// Response message
{
  "timestamp": "<epoch>",
  "previousTimestamp": "<epoch>",
  "productId": "<uuid>",
  "asks": [[price: string, quantity: string]],
  "bids": [[price: string, quantity: string]]
}
```

* `BookDepth` events are emitted on a configurable fixed interval (as of writing, this is configured to be **once every 200ms**)
* `previousTimestamp` is in milliseconds and represents the last time the `BookDepth` emitted
* `timestamp` also in milliseconds and the system timestamp of when this `BookDepth`was emitted
* Using both the `previousTimestamp` and `timestamp` you can infer whether or not any events were missed during connection or during consumption
* `asks` an array of price/quantity tuples representing asks
* `bids` an array of price/quantity tuples representing bids

{% hint style="warning" %}
A `BookDepth` message of the current book **(up to 100 price levels per side)** is emitted back on initial connection. Every subsequent message is a price level diff with absolute quantities. A zero quantity price diff indicates that this level has been removed.
{% endhint %}

**`MARKET_PRICE`** - Delivers real-time market price updates for a specified product.

```json
// Subscription message payload
{
  "type": "MarketPrice",
  "productId": "<uuid>"
}

// Response message (same as `/v1/product/market-price`)
// @see: https://api.ethereal.trade/docs#/Product/ProductController_getMarketPrice
{
  "productId": "<uuid>",
  "bestBidPrice": numberString,
  "bestAskPrice": numberString,
  "oraclePrice": numberString,
  "price24hAgo": numberString
}
```

* `MarketPrice` events are emitted on a configurable fixed interval (currently configured to be **once every second**)

**`ORDER_FILL`** - Notifies when orders are filled for a specific subaccount.

```json
// Subscription message payload
{
  "type": "OrderFill",
  "subaccountId": "<uuid>"
}

// Response message (same as `/v1/order/fill`)
// @see: https://api.ethereal.trade/docs#/Order/OrderController_listFillsBySubaccountId
{
  "data": [
      {
        "id": "<uuid>",
        "orderId": "<uuid>",
        "clientOrderId": "string",
        "price": numberString,
        "filled": numberString,
        "type": "LIMIT|MARKET",
        "side": 0|1,
        "reduceOnly": boolean,
        "feeUsd": numberString,
        "isMaker": boolean,
        "productId": "<uuid>",
        "subaccountId": "<uuid>",
        "createdAt": "<epoch>"
      }
    ]
}
```

* `OrderFill` events are emitted in real-time as they occur

**`TRADE_FILL`** - Provides a stream of trades that have occurred filtered by product.

```json
// Subscription message payload
{
  "type": "TradeFill",
  "productId": "<uuid>"
}

// Response message
{
  "data": [
    {
      "id": "<uuid>",
      "price": numberString,
      "filled": numberString,
      "takerSide": 0|1,
      "createdAt": "<epoch>"
    }
  ],
  "productId": "<uuid>"
}
```

* `TradeFill` events are emitted in real-time as they occur
* Similar to `OrderFill`, an array of trade fills will be emitted in a single message, grouped by the product they were traded on

**`ORDER_UPDATE`** - Provides updates about order status changes for a specific subaccount.

```json
// Subscription message payload
{
  "type": "OrderUpdate",
  "subaccountId": "<uuid>"
}

// Response message (same as `/v1/order`)
// @see https://api.ethereal.trade/docs#/Order/OrderController_listBySubaccountId
{
  "data": [
    {
      "id": "<uuid>",
      "clientOrderId": "string",
      "type": "LIMIT"|"MARKET",
      "availableQuantity": numberString,
      "quantity": numberString,
      "side": 0|1,
      "productId": "<uuid>",
      "subaccountId": "<uuid>",
      "status": "<status>",
      "reduceOnly": boolean,
      "close": booleanue,
      "updatedAt": "<epoch>",
      "createdAt": "<epoch>",
      "sender": "<address>",
      "price": numberString,
      "filled": numberString,
      "stopPrice": numberString,
      "stopType": "<stopType>",
      "stopPriceType": "<stopPriceType>",
      "timeInForce": "<tif>",
      "expiresAt": "<epochInSeconds>",
      "postOnly": boolean,
      "groupContingencyType": "<groupContingencyType>",
      "groupId": "<uuid>"
    }
  ]
}

```

* `OrderUpdate` events are emitted in real-time

{% hint style="warning" %}
Only the latest update processed is emitted and intermediary states are omitted.
{% endhint %}

**`SUBACCOUNT_LIQUIDATION`** - Provides an update when a subaccount is liquidated.

```json
// Subscription message payload
{
  "type": "SubaccountLiquidation",
  "subaccountId": "<uuid>"
}

// Response message
{
  "subaccountId": "<uuid>",
  "liquidatedAt": "<epoch>"
}
```

* `SubaccountLiquidation` events are emitted in real-time
* `subaccountId` the subaccount that has been liquidated, `liquidatedAt` the time (in ms, since the Unix epoch) when the liquidation occurred

**`TOKEN_TRANSFER`** - Updates on token transfers (deposits/withdrawals) for a specific subaccount.

```json
// Subscription message payload
{
  "type": "TokenTransfer",
  "subaccountId": "<uuid>"
}

// Response message (same as `/v1/token/transfer` with pagination)
// @see https://api.ethereal.trade/docs#/Token/TokenController_listTransfers
{
  "id": "<uuid>",
  "initiatedBlockNumber": numberString,
  "finalizedBlockNumber": numberString,
  "status": "<status>",
  "subaccountId": "<uuid>",
  "tokenName": "string",
  "tokenAddress": "<address>",
  "type": "WITHDRAW"|"DEPOSIT",
  "amount": numberString,
  "lzDestinationAddress": "<address>",
  "lzDestinationEid": number,
  "fee": numberString,
  "createdAt": "<epoch>",
  "initiatedTransactionHash": "<hex>",
  "finalizedTransactionHash": "<hex>"
}
```

{% hint style="success" %}
*Each subscription requires specific parameters as shown in the formats above. To subscribe to these streams, emit a 'subscribe' event to the socket with the appropriate subscription message.*
{% endhint %}

{% hint style="warning" %}
During connection establishment ensure `websocket` is the only configured transport (i.e. `transports: ['websocket']`).
{% endhint %}

### Unsubscribe

To stop receiving data from a previously established subscription, you can unsubscribe using the same payload format as your original subscription. Simply emit an `unsubscribe` event to the socket with the identical payload structure you used when subscribing.

### Example Code Snippets

Below is an example implemented using TypeScript and `socket.io-client`.

```typescript
import { io } from 'socket.io-client';
import axios from 'axios';
import { getEtherealTestnetAPI } from './generated/api.sdk';

axios.defaults.baseURL = 'https://api.etherealtest.net';
const api = getEtherealTestnetAPI(); // Generated SDK using Orval

const getProducts = async () => {
  const res = await api.productControllerList();
  return res.data.data;
};

const main = async () => {
  const url = 'wss://ws.etherealtest.net/v1/stream';
  const ws = io(url, { transports: ['websocket'], autoConnect: false });
  console.log(`Connecting to ws gateway ${url}`);

  ws.on('connect', async () => {
    console.log(`Connected to ${url}`);

    const products = await getProducts();

    products.forEach((product) => {
      const bookDepthSubscriptionMessage = {
        type: 'BookDepth',
        productId: product.id,
      };
      ws.emit('subscribe', bookDepthSubscriptionMessage);
      console.log(`Subscribed BookDepth:${product.id}`);

      const marketPriceSubscriptionMessage = {
        type: 'MarketPrice',
        productId: product.id,
      };
      ws.emit('subscribe', marketPriceSubscriptionMessage);
      console.log(`Subscribed MarketPrice:${product.id}`);
    });
  });

  ws.on('connecting', () => console.log('Attempting connection...'));
  ws.on('disconnect', () => console.log('Disconnected'));
  ws.on('error', (err) => console.log('Error encountered', err));
  ws.on('exception', (err) => console.log('Caught exception', err));
  ws.on('reconnect_attempt', () => console.log('Attempting to reconnect...'));

  // --- Subscription stream handlers --- //

  ws.on('BookDepth', async (message) => console.log(`[BookDepth] Received ${message}`));
  ws.on('MarketPrice', async (message) => console.log(`[MarketPrice] Received ${message}`));

  // Explicitly connect to ws stream _after_ binding message callbacks.
  ws.connect();
};

void main();
```

Another example using Java to listen on `BookDepth`:

```java
package com.example.ethereal;

import io.socket.client.IO;
import io.socket.client.Socket;
import io.socket.emitter.Emitter;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Example {
    private Socket client;
    private CountDownLatch connectionLatch = new CountDownLatch(1);

    public static void main(String[] args) {
        EtherealClient client = new EtherealClient();

        try {
            client.connect();
            client.subscribeToBookDepth("6dae67f4-c502-4cc1-8d1a-38ab021b2c76");

            // Keep running for 30 seconds
            Thread.sleep(30000);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            client.disconnect();
        }
    }

    public void connect() throws InterruptedException {
        System.out.println("Connecting to Ethereal Testnet WebSocket...");

        URI serverUri = URI.create("wss://ws.etherealtest.net/v1/stream");

        IO.Options options = new IO.Options();
        options.transports = new String[]{"websocket"};
        options.upgrade = false;
        options.timeout = 10000;

        client = IO.socket(serverUri, options);

        client.on(Socket.EVENT_CONNECT, new Emitter.Listener() {
            @Override
            public void call(Object... args) {
                System.out.println("Connected! Socket ID: " + client.id());
                connectionLatch.countDown();
            }
        });

        client.on(Socket.EVENT_CONNECT_ERROR, new Emitter.Listener() {
            @Override
            public void call(Object... args) {
                System.err.println("Connection failed: " + Arrays.toString(args));
                connectionLatch.countDown();
            }
        });

        client.on("BookDepth", new Emitter.Listener() {
            @Override
            public void call(Object... args) {
                System.out.println("BookDepth: " + Arrays.toString(args));
            }
        });

        client.on("exception", new Emitter.Listener() {
            @Override
            public void call(Object... args) {
                System.err.println("Server exception: " + Arrays.toString(args));
            }
        });

        client.connect();

        if (!connectionLatch.await(15, TimeUnit.SECONDS)) {
            throw new RuntimeException("Connection timeout");
        }
    }

    public void subscribeToBookDepth(String productId) {
        if (!client.connected()) {
            System.err.println("Not connected!");
            return;
        }

        Map<String, Object> request = new HashMap<>();
        request.put("type", "BookDepth");
        request.put("productId", productId);

        System.out.println("Subscribing: " + request);
        client.emit("subscribe", request);
    }

    public void disconnect() {
        if (client != null) {
            System.out.println("Disconnecting...");
            client.disconnect();
            client.close();
        }
    }
}
```

For Python developers, our Python SDK offers a streamlined approach to WebSocket subscriptions Refer to the [Python SDK](https://docs.ethereal.trade/developer-guides/python-sdk) documentation for more information.

### Handling WS Exceptions

Exceptions are exposed in its own event aptly named "*exception*". Exceptions follow the following shape:

```typescript
{
  pattern: 'order:dryRun',
  status: 'BadRequest',
  error: {
    message: [
      'subaccount is not a valid subaccount',
    ]
  }
}
```

* `pattern` indicates the source event pattern if available e.g. "subscribe"&#x20;
* `status` the error status
* `error` general body of the error its shape changes depending on the status
