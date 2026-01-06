# Products

## Overview

Products are the financial instruments available for trading within the exchange. Each product represents a distinct market with specific trading rules and risk parameters. At launch, Ethereal will offer only perpetual futures contracts, which serve as the foundation for all trading activity on the platform.

### **Product Configuration**

Perpetual futures are derivative contracts that track underlying asset prices without expiration dates. Key specifications include contract size, minimum tick size, maximum leverage ratios, and funding rate mechanisms that align contract prices with spot markets. Each contract defines margin requirements, position limits, and liquidation parameters for risk management.

Product configuration is mostly smart contract driven. All smart contract product and product related configuration updates emit an event, an offchain indexer consumes this event and propagates changes downstream.

You can query for product details directly in the smart contracts or through the API.

```bash
curl -X 'GET' \
  'https://api.ethereal.trade/v1/product?order=asc&orderBy=createdAt' \
  -H 'accept: application/json'
```

```json
{
  "data": [
    {
      "id": "bc7d5575-3711-4532-a000-312bfacfb767",
      "ticker": "BTCUSD",
      "displayTicker": "BTC-USD",
      "engineType": 0,
      "onchainId": 1,
      "blockNumber": "18539",
      "baseTokenAddress": "0x0000000000000000000000000000000000000000",
      "quoteTokenAddress": "0xb6fc4b1bff391e5f6b4a3d2c7bda1fee3524692d",
      "baseTokenName": "BTC",
      "quoteTokenName": "USD",
      "lotSize": "0.00001",
      "tickSize": "1",
      "makerFee": "0",
      "takerFee": "0.0003",
      "maxQuantity": "5",
      "minQuantity": "0.00008",
      "minPrice": "1",
      "maxPrice": "10112491",
      "volume24h": "0",
      "createdAt": 1760542037979,
      "cumulativeFundingUsd": "-73.074331773",
      "fundingUpdatedAt": 1760760000001,
      "fundingRate1h": "0.00022831",
      "openInterest": "0",
      "maxLeverage": 20,
      "maxOpenInterestUsd": "25000000",
      "maxPositionNotionalUsd": "2500000",
      "pythFeedId": 1
    },
    {
      "id": "480014cc-536e-4fd4-958b-b2afcf8ce09f",
      "ticker": "ETHUSD",
      "displayTicker": "ETH-USD",
      "engineType": 0,
      "onchainId": 2,
      "blockNumber": "18539",
      "baseTokenAddress": "0x0000000000000000000000000000000000000000",
      "quoteTokenAddress": "0xb6fc4b1bff391e5f6b4a3d2c7bda1fee3524692d",
      "baseTokenName": "ETH",
      "quoteTokenName": "USD",
      "lotSize": "0.0001",
      "tickSize": "0.1",
      "makerFee": "0",
      "takerFee": "0.0003",
      "maxQuantity": "100",
      "minQuantity": "0.0024",
      "minPrice": "0.1",
      "maxPrice": "1004105",
      "volume24h": "0",
      "createdAt": 1760542037979,
      "cumulativeFundingUsd": "0",
      "fundingRate1h": "0",
      "openInterest": "0",
      "maxLeverage": 20,
      "maxOpenInterestUsd": "25000000",
      "maxPositionNotionalUsd": "2500000",
      "pythFeedId": 2
    },
    {
      "id": "628098e9-f7a3-4ae7-9996-149aac4ca435",
      "ticker": "SOLUSD",
      "displayTicker": "SOL-USD",
      "engineType": 0,
      "onchainId": 3,
      "blockNumber": "18541",
      "baseTokenAddress": "0x0000000000000000000000000000000000000000",
      "quoteTokenAddress": "0xb6fc4b1bff391e5f6b4a3d2c7bda1fee3524692d",
      "baseTokenName": "SOL",
      "quoteTokenName": "USD",
      "lotSize": "0.001",
      "tickSize": "0.01",
      "makerFee": "0",
      "takerFee": "0.0003",
      "maxQuantity": "500",
      "minQuantity": "0.049",
      "minPrice": "0.01",
      "maxPrice": "100203",
      "volume24h": "0",
      "createdAt": 1760542038423,
      "cumulativeFundingUsd": "0",
      "fundingRate1h": "0",
      "openInterest": "0",
      "maxLeverage": 10,
      "maxOpenInterestUsd": "5000000",
      "maxPositionNotionalUsd": "500000",
      "pythFeedId": 6
    }
  ],
  "hasNext": false
}
```

### Market Prices

Ethereal exclusively utilizes **Pyth Lazer** as our oracle provider to provide markets with mark prices. Pyth Lazer delivers high-quality, low-latency price feeds that power critical functions across our trading infrastructure. You can learn more about Pyth Lazer by visiting [their site](https://www.pyth.network/pyth-price-feeds) or reading their [docs](https://docs.pyth.network/lazer).

Oracle prices from Pyth Lazer serve two primary functions within Ethereal.

### Retrieving Relevant Market Prices

Ethereal trading API provides access to last mark (oracle), best bid, and best ask prices either through the HTTP API or websockets.

```bash
curl -X 'GET' \
  'https://api.ethereal.trade/v1/product/market-price?productIds=d32ea32d-1313-4c64-9ef2-aed82e592a3c' \
  -H 'accept: application/json'
```

```json
{
  "data": [
    {
      "productId": "d32ea32d-1313-4c64-9ef2-aed82e592a3c",
      "bestBidPrice": "1807.200000000",
      "bestAskPrice": "1807.700000000",
      "oraclePrice": "1807.36431582",
      "price24hAgo": "1816.47086747"
    }
  ]
}
```

Please refer to [Websocket Subscriptions](https://docs.ethereal.trade/developer-guides/trading-api/websocket-gateway) section for details on how you can receive price updates via websockets.
