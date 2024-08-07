import express from "express";
import { Client } from 'pg';
import dotenv from 'dotenv';
import { ethers } from 'ethers';
import { Web3 } from 'web3';
import { Network, Alchemy, AlchemySubscription } from "alchemy-sdk";
import Decimal from 'decimal.js';
import './logger';
import {
  addAlchemyContextToRequest,
  validateAlchemySignature,
  getCurrentTimeISOString,
  Save,
  getEthereumTokenUSD
} from "./webhooksUtil";

dotenv.config();

const alchemy_keys = process.env.ALCHEMY_API_KEY?.split(',');
const alchemy_keys_for_web3 = process.env.ALCHEMY_API_KEY_FOR_WEB3?.split(',');

const prod_client = new Client({
  host: '18.188.193.193',
  database: 'postgres',
  user: 'myuser',
  password: 'Lapis@123',
  port: 5432,
});

// prod_client.connect((err) => {
//   if (err) {
//     console.error('Connection error', err.stack);
//   } else {
//     console.log('Connected to the prod_client database');
//   }
// });

const client = new Client({
  host: 'trading.copaicjskl31.us-east-2.rds.amazonaws.com',
  database: 'trading',
  user: 'creative_dev',
  password: '4hXWW1%G$',
  port: 5000,
  ssl: {
    rejectUnauthorized: false, // Bypass certificate validation
  },
});

client.connect((err) => {
  if (err) {
    console.error('Connection error', err.stack);
  } else {
    console.log('Connected to the database');
  }
});


var settings = {
  apiKey: alchemy_keys[0],
  network: Network.ETH_MAINNET,
};

var alchemy = new Alchemy(settings);
var web3 = new Web3(`https://eth-mainnet.alchemyapi.io/v2/${alchemy_keys[0]}`);
var web3s: Web3[] = []

for (const akey of alchemy_keys_for_web3) {
  web3s.push(new Web3(`https://eth-mainnet.alchemyapi.io/v2/${akey}`));
}

async function switchAlchemyAPI() {
  const hours = (new Date()).getHours();
  if (settings.apiKey == alchemy_keys[parseInt(hours / (24 / alchemy_keys.length))]) {
    return false;
  }
  console.log(`Switching Alcehmy API Key to ${alchemy_keys[parseInt(hours / (24 / alchemy_keys.length))]} from ${settings.apiKey}!`);
  settings.apiKey = alchemy_keys[parseInt(hours / (24 / alchemy_keys.length))];
  alchemy = new Alchemy(settings);
  web3 = new Web3(`https://eth-mainnet.alchemyapi.io/v2/${alchemy_keys[parseInt(hours / (24 / alchemy_keys.length))]}`);
  return true;
}

const main = async () => {
  const app = express();

  //await switchAlchemyAPI();

  const port = process.env.PORT;
  const host = process.env.HOST;
  const signingKey = process.env.WEBHOOK_SIGNING_KEY;

  // Middleware needed to validate the alchemy signature
  app.use(
    express.json({
      limit: '100mb',
      verify: addAlchemyContextToRequest,
    })
  );
  app.use(validateAlchemySignature(signingKey));

  const UNISWAP_V3_SWAP_EVENT = '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67';
  const UNISWAP_V2_SWAP_EVENT = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822';
  const TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
  const WETH = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'

  var logs: {}[] = [];

  let timer: NodeJS.Timeout | null = null;
  let timer_ws: NodeJS.Timeout | null = null;
  let timer_ws_tx: NodeJS.Timeout | null = null;
  var PARSING: Boolean = false;
  var ARRIVING: Boolean = false;
  var block_timestamp: string;
  var ETH_LATEST_PRICE: Decimal;
  var lastBlockNumberWithETH: number = 0;
  var hashes: Array<string> = [];
  const hash2wallet = new Map<string, string>();

  async function parseSwapEvents() {
    const switched = await switchAlchemyAPI();
    if (switched) {
      await connectTxWebsocket();
      await connectWebsocket();
    }
    if (logs.length == 0) return;
    PARSING = true;
    ARRIVING = false;
    const currentBlockNumber = logs[0].blockNumber;
    var _logs = logs.filter(log => log.blockNumber == currentBlockNumber);
    logs = logs.filter(log => log.blockNumber != currentBlockNumber);
    var start_time: Date = new Date();
    console.log(`started parsing block:${currentBlockNumber} at: ` + getCurrentTimeISOString());
    // Fetch ETH price
    // if (currentBlockNumber - lastBlockNumberWithETH >= 1) {
    //   const eth_current_price = await getEthereumTokenUSD('0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2');
    //   if (!eth_current_price.isZero()) {
    //     ETH_LATEST_PRICE = eth_current_price;
    //     lastBlockNumberWithETH = currentBlockNumber;
    //   }
    // }
    // console.log(`Current ETH Price ${ETH_LATEST_PRICE}`);
    // if (ETH_LATEST_PRICE == undefined) {
    //   console.log(`Skipping block ${currentBlockNumber} due to undefined ETH price`);
    //   PARSING = false;
    //   return;
    // }
    // console.log(`fetched ETH USD of block ${currentBlockNumber} at: ` + getCurrentTimeISOString());
    // Example: Extract token swap details

    var currentTransactionhash: string = '';
    var currentFromAddress: string = '';

    var swapEvents = [];

    var token0: string = null;
    var token1: string = null;

    var yes = 0;
    var no = 0;

    var hashes: {}[] = [];
    for (var i = 0; i < _logs.length; ++i) {
      var amount0, amount1;
      if (_logs[i].topics[0] == TRANSFER) {
        token0 = token1;
        token1 = _logs[i].address;
      }
      if (_logs[i].topics[0] == UNISWAP_V3_SWAP_EVENT) {
        const iface = new ethers.Interface([
          'event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)'
        ]);

        const parsedLog = iface.parseLog(_logs[i]);
        amount0 = parsedLog?.args.amount0;
        amount1 = parsedLog?.args.amount1;
      } else if (_logs[i].topics[0] == UNISWAP_V2_SWAP_EVENT) {
        const iface = new ethers.Interface([
          'event Swap(address indexed sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address indexed to)'
        ]);

        const parsedLog = iface.parseLog(_logs[i]);
        const amount0In = parsedLog?.args.amount0In;
        const amount0Out = parsedLog?.args.amount0Out;
        const amount1In = parsedLog?.args.amount1In;
        const amount1Out = parsedLog?.args.amount1Out;
        if (amount0In == 0 || amount1Out == 0) {
          amount0 = -amount0Out;
          amount1 = amount1In;
        } else {
          amount0 = amount0In;
          amount1 = amount1Out;
        }
      } else {
        continue;
      }

      if (token0 != WETH && token1 != WETH)
        continue;
      var amount0Decimal = new Decimal(ethers.formatUnits(amount0, 18));
      var amount1Decimal = new Decimal(ethers.formatUnits(amount1, 18));
      _logs[i].token0 = _logs[i].token1 = {};
      if (amount0Decimal.isPositive()) {
        _logs[i].token0 = {
          id: token0,
          amount: amount0Decimal,
        };
        _logs[i].token1 = {
          id: token1,
          amount: amount1Decimal.abs(),
        };
      } else {
        _logs[i].token0 = {
          id: token1,
          amount: amount1Decimal,
        };
        _logs[i].token1 = {
          id: token0,
          amount: amount0Decimal.abs(),
        }
      }
      if (_logs[i].token0.id != WETH)
        _logs[i].token0.amount = null;
      else
        _logs[i].token1.amount = null;
      if (_logs[i].transactionHash != currentTransactionhash) {
        currentTransactionhash = _logs[i].transactionHash;
        if (hash2wallet.has(currentTransactionhash)) {
          yes += 1;
          currentFromAddress = hash2wallet.get(currentTransactionhash);
          _logs[i].fromAddress = currentFromAddress;
        } else {
          no += 1;
          hashes.push({ "hash": currentTransactionhash, "id": swapEvents.length, "web3": web3s[parseInt((no - 1) / 5)] })
        }
      } else {
        _logs[i].fromAddress = currentFromAddress;
      }
      swapEvents.push(_logs[i]);
    }
    var failed = 0;
    try {
      const transactionPromises = hashes.map(hash => hash.web3.eth.getTransaction(hash.hash));

      const transactions = await Promise.all(transactionPromises);
      const h2w = new Map<string, string>();
      for (var txid = 0; txid < transactions.length; txid += 1) {
        const currentFromAddress = transactions[txid]?.from;
        h2w.set(hashes[txid].hash, currentFromAddress);
        if(transactions[txid].from == null)
          failed += 1;
      };
      for (var sid = 0; sid < swapEvents.length; ++ sid) {
        if (h2w.has(swapEvents[sid].transactionHash)) {
          swapEvents[sid].fromAddress = h2w.get(swapEvents[sid].transactionHash);
        }
      }
    } catch (error) {
      console.error('Error fetching transactions:', error);
    }
    console.log(`${yes + no}:${yes}:${no}:${failed}`);
    console.log(`started storing block ${currentBlockNumber} into db at: ` + getCurrentTimeISOString());
    await Save(swapEvents, client, web3, prod_client);
    console.log(`finished block ${currentBlockNumber} in ${(((new Date()).getTime() - start_time.getTime()) / 1000.0)} seconds`);
    PARSING = false;
  }

  var filter = {
    addresses: [

    ],
    topics: [
      [UNISWAP_V3_SWAP_EVENT, UNISWAP_V2_SWAP_EVENT, TRANSFER]
    ]
  }

  async function connectTxWebsocket() {
    console.log("connecting websocket for tx");
    if (timer_ws_tx) {
      clearTimeout(timer_ws_tx);
    }
    timer_ws_tx = setTimeout(connectTxWebsocket, 2 * 1000);
    alchemy.ws.on(
      {
        method: AlchemySubscription.PENDING_TRANSACTIONS,
      },
      (tx) => {
        //console.log(tx);
        if (hashes.length && hashes.length > 1000) {
          const oldHash = hashes.shift();
          hash2wallet.delete(oldHash);
        }
        hashes.push(tx.hash);
        hash2wallet.set(tx.hash, tx.from);
        if (timer_ws_tx) {
          clearTimeout(timer_ws_tx);
        }
        timer_ws_tx = setTimeout(connectTxWebsocket, 2 * 1000);
      }
    );
  }

  async function connectWebsocket() {
    console.log("connecting websocket");
    if (timer_ws) {
      clearTimeout(timer_ws);
    }
    timer_ws = setTimeout(connectWebsocket, 15 * 1000);
    alchemy.ws.on(filter, async (log) => {
      if (!ARRIVING) {
        ARRIVING = true;
        console.log("================");
        block_timestamp = getCurrentTimeISOString();
        console.log(`arrived block:${log.blockNumber} at: ` + block_timestamp);
        console.log(`Alchemy API Key: ${alchemy.config.apiKey}`);
      }
      if (timer) {
        clearTimeout(timer);
      }
      timer = setTimeout(parseSwapEvents, 300);
      logs.push(log);
      if (timer_ws) {
        clearTimeout(timer_ws);
      }
      timer_ws = setTimeout(connectWebsocket, 15 * 1000);
    })
  }

  await connectTxWebsocket();
  await connectWebsocket();
  // Listen to Alchemy Notify webhook events
  app.listen(port, host, () => {
    console.log(`Example Alchemy Notify app listening at ${host}:${port}`);
  });
}

main();