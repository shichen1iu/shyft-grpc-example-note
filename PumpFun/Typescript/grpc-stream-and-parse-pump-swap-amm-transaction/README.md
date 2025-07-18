# Stream and parse Pumpswap AMM transaction instructions via gRPC

This project provides a Node.js service to fetch and parse transactions from Pumpswap AMM using gRPC.

## Features
- Uses Solana gRPC API for efficient transaction retrieval.
- Parses transactions using `@shyft-to/solana-transaction-parser`.
- Supports `.env` configuration for easy authentication.

## Installation
```sh
git clone https://github.com/Shyft-to/solana-defi.git
cd grpc-stream-and-parse-pump-swap-amm-transaction
npm install
```

## Configuration
Create a `.env` file in the root directory based on `.env.example`:
```
ENDPOINT=https://grpc.ams.shyft.to
X_TOKEN=YOUR_AUTH_TOKEN
```

## Usage
### Development
```sh
npm run watch
```

### Build
```sh
npm run build
```

### Run
```sh
npm start
```

## Dependencies
- `@solana/web3.js` for interacting with Solana blockchain.
- `@triton-one/yellowstone-grpc` for gRPC communication.
- `@shyft-to/solana-transaction-parser` for transaction parsing.

## License
This project is licensed under the [MIT License](LICENSE).

## Author
[impin2rex](https://impin2rex.github.io)
