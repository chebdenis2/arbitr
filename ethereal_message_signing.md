# Message Signing

## Overview

When interacting with the Ethereal exchange, many operations like trading and account management require cryptographic signatures to authenticate and authorize your actions.

### Why Sign Messages?

The trading API uses cryptographic signatures for authentication instead of traditional JSON Web Tokens (JWTs). When making an API request, rather than including a JWT obtained from a centralized authentication service, the requesting client signs the an EIP-712 structured message using their private key. The signature proves the client's identity and authorizes them to access the endpoint.

Majority of endpoints are read-only public facing. However, for endpoints that mutate data such as order submissions and cancelations, these calls are authenticated and authorised via signatures in the form of EIP-712 messages (<https://eips.ethereum.org/EIPS/eip-712>).

Each authenticated endpoint requires a different message type to sign. Ethereal has a few message types including: `LinkSigner`, `RevokeLinkedSigner`, `InitiateWithdraw`, `TradeOrder` , and `CancelOrder`. Once a signature is created, they are sent along with the rest of the HTTP payload, validated, stored, and the relayer batches these operations onchain at a later time.

## Signature Types and Domain

Message signing represents a technical intersection where internal message structures meet external API usage. We understand this can be one of the more challenging aspects of our developer experience. To help, we've created several utilities and helper endpoints to minimise implementation complexity.

```bash
curl -X 'GET' \
  'https://api.ethereal.trade/v1/rpc/config' \
  -H 'accept: application/json'
```

```json
{
  "domain": {
    "name": "Ethereal",
    "version": "1",
    "chainId": 5064014,
    "verifyingContract": "0xB3cDC82035C495c484C9fF11eD5f3Ff6d342e3cc"
  },
  "signatureTypes": {
    "LinkSigner": "address sender,address signer,bytes32 subaccount,uint64 nonce,uint64 signedAt",
    "TradeOrder": "address sender,bytes32 subaccount,uint128 quantity,uint128 price,bool reduceOnly,uint8 side,uint8 engineType,uint32 productId,uint64 nonce,uint64 signedAt",
    "InitiateWithdraw": "address account,bytes32 subaccount,address token,uint256 amount,uint64 nonce,uint64 signedAt",
    "UpdateFunding": "uint32 productId,int128 fundingDeltaUsd",
    "RevokeLinkedSigner": "address sender,address signer,bytes32 subaccount,uint64 nonce,uint64 signedAt",
    "CancelOrder": "address sender, bytes32 subaccount, uint64 nonce"
  }
}
```

{% hint style="warning" %}
This configuration may not reflect the current state. Query the live API endpoint `/v1/rpc/config` to retrieve the latest version.
{% endhint %}

There are 2 components to this response: **domain** and **signatureTypes**.

### Domain

The `domain` object provides context for the signed message and helps prevent cross-application replay attacks. It includes:

* `name`: The name of the signing application or protocol (e.g., "Ethereal")
* `version`: The current version of the contract/application
* `chainId`: The chain ID where the signature is valid
* `verifyingContract`: The address of the contract that will verify the signature

This domain information creates a unique context for each application, ensuring that signatures created for one application cannot be reused in another.

### Signature Types

The `signatureTypes`, `messageTypes` (or `types`) defines the structure of the data being signed. It's an object containing named structures with their respective fields and types. In the example above, `LinkSigner` has the following shape:

```
address sender,address signer,bytes32 subaccount,uint64 nonce,uint64 signedAt
```

Which, when parsed gives the following:

```typoscript
[
    { name: 'sender', type: 'address' },
    { name: 'signer', type: 'address' },
    { name: 'subaccount', type: 'bytes32' },
    { name: 'nonce', type: 'uint64' },
    { name: 'signedAt', type: 'uint64' },
]
```

{% hint style="warning" %}
Messages are raw and internal to the system (but partially exposed due to message signing). They will likely be changed and improved over time to make message signing simpler before mainnet launch. However, once launched on mainnet, they will remain the same.

Should they change, we will introduce a new message type, deprecate the old, and migrate to the new.
{% endhint %}

### What is Nonce?

Every message type includes a `uint64 nonce`. On Ethereal, the nonce functions as a uniqueness parameter that prevents replay attacks by ensuring each signed message can only be processed once. Unlike traditional implementations using sequential counters, Ethereal uses the current timestamp in nanoseconds, providing a high-precision identifier that guarantees no two legitimate transactions will share the same nonce, even when submitted rapidly.

When signing orders or executing operations, this nanosecond timestamp becomes part of the signed data structure. The exchange validates each signature by verifying the timestamp is within an acceptable window and hasn't been previously processed, automatically rejecting any attempt to reuse a signature. This approach supports high-frequency trading without compromising security, as users can generate multiple valid signatures quickly without tracking on-chain state changes.

To generate a nonce, we recommend simply just retrieving the current time in nanoseconds and adding some randomness at the end of the timestamp.

### Message Expiry

Message nonces are tracked to prevent reuse, though there can be delays between message signing and onchain verification. To enhance security, messages include expiry mechanisms that prevent potential replay attacks over extended periods. Since verification occurs onchain, expired messages remain protected even in compromised scenarios.

{% hint style="info" %}
If your programming language of choice is Python, the [Python SDK](https://docs.ethereal.trade/developer-guides/python-sdk) has signing utility functions to assist with message signing.
{% endhint %}
