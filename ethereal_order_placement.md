# Order Placement

## Overview

The platform supports two primary order types: **market orders** for immediate execution and **limit orders** for execution at specific price levels. Before placing any order, you must have sufficient margin deposited in your subaccount to cover the position. Once placed, the required margin is immediately locked regardless of whether the order has been filled, ensuring adequate collateral throughout the order's lifetime. For detailed information about order mechanics and advanced order types, [refer to the trading documentation](https://docs.ethereal.trade/trading/perpetual-futures/order-types).

## Lifecycle

Every order follows a complete lifecycle from initial creation through final disposition within the trading system. You can monitor an order's current state by checking the `status` field in the when querying for orders, which provides real-time updates as the order progresses through different stages. Understanding these status transitions helps you track order execution and manage your trading strategy effectively.

The order status system includes several key states that indicate where your order stands in the execution process.

<table><thead><tr><th width="173.66015625">Status</th><th>Description</th></tr></thead><tbody><tr><td><strong><code>NEW</code></strong></td><td>A submitted order that has successfully submitted, acknowledged and visible on the books.</td></tr><tr><td><strong><code>PENDING</code></strong></td><td>An acknowledged order that has not yet been triggered.</td></tr><tr><td><strong><code>FILLED_PARTIAL</code></strong></td><td>A partially filled order.</td></tr><tr><td><strong><code>FILLED</code></strong></td><td>A closed order that has been fully filled.</td></tr><tr><td><strong><code>CANCELED</code></strong></td><td>A canceled order that may have been partially filled or not. You can identify which state by consuming the <code>filled</code> property. A non-zero filled value and canceled status is the former whereas a zero filled is the latter.</td></tr><tr><td><strong><code>EXPIRED</code></strong></td><td>An order becomes expired when the current system time reaches or exceeds the <code>expireTime</code> timestamp that was assigned during submission, automatically removing it from the order book.</td></tr></tbody></table>

{% hint style="info" %}
After successfully submitting an order, the API returns the order's current state in real-time, including the order details and any quantity that was immediately filled during submission.
{% endhint %}
