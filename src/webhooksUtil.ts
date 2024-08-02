import { NextFunction } from "express";
import { Request, Response } from "express-serve-static-core";
import axios from 'axios';
import * as crypto from "crypto";
import { IncomingMessage, ServerResponse } from "http";
import { Web3 } from 'web3'
import Decimal from 'decimal.js'
import { Client } from 'pg';
import * as fs from 'fs';
import './logger';

export interface AlchemyRequest extends Request {
  alchemy: {
    rawBody: string;
    signature: string;
  };
}

export function isValidSignatureForAlchemyRequest(
  request: AlchemyRequest,
  signingKey: string
): boolean {
  return isValidSignatureForStringBody(
    request.alchemy.rawBody,
    request.alchemy.signature,
    signingKey
  );
}

export function isValidSignatureForStringBody(
  body: string,
  signature: string,
  signingKey: string
): boolean {
  const hmac = crypto.createHmac("sha256", signingKey); // Create a HMAC SHA256 hash using the signing key
  hmac.update(body, "utf8"); // Update the token hash with the request body using utf8
  const digest = hmac.digest("hex");
  return signature === digest;
}

export function addAlchemyContextToRequest(
  req: IncomingMessage,
  _res: ServerResponse,
  buf: Buffer,
  encoding: BufferEncoding
): void {
  const signature = req.headers["x-alchemy-signature"];
  // Signature must be validated against the raw string
  var body = buf.toString(encoding || "utf8");
  (req as AlchemyRequest).alchemy = {
    rawBody: body,
    signature: signature as string,
  };
}

export function validateAlchemySignature(signingKey: string) {
  return (req: Request, res: Response, next: NextFunction) => {
    if (!isValidSignatureForAlchemyRequest(req as AlchemyRequest, signingKey)) {
      const errMessage = "Signature validation failed, unauthorized!";
      res.status(403).send(errMessage);
      throw new Error(errMessage);
    } else {
      next();
    }
  };
}

export const getEthereumTokenUSD = async (token_address: string) => {
  try {
    // const response = await axios.get(`https://api.coingecko.com/api/v3/coins/ethereum/contract/${token_address}`);

    // return new Decimal(response.data.market_data.current_price.usd);
    const headers = {
      'accept': 'application/json, multipart/mixed',
      'accept-language': 'en-US,en;q=0.9',
      'authorization': 'a8d3b922af01a77d58eccda22095efbbef616670',
      'content-type': 'application/json',
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }
    const json_data = {
      'query': '{\n  filterTokens(\n    filters: {\n      network: [1]\n    }\n    limit: 200\n    tokens:["' + token_address + '"]\n  ) {\n    results {\n      change1\n      change4\n      change12\n      change24\n      createdAt\n      volume1\n      volume4\n      volume12\n      isScam\n      holders\n      liquidity\n      marketCap\n      priceUSD\n      volume24\n      pair {\n        token0Data{symbol}\n        token1Data{symbol}\n        address\n      }\n      exchanges {\n        address\n      }\n      token {\n        address\n        decimals\n        name\n        networkId\n        symbol\n        \n      }\n    }\n  }\n}',
    };

    const response = await axios.post('https://graph.defined.fi/graphql', json_data, {headers});
    return new Decimal(response.data.data.filterTokens.results[0].priceUSD);
  } catch (e) {
    console.error(e);
    return new Decimal(0);
  }
}

function addEdge(graph: Map<string, { id: string, ratio: Decimal }[]>, A: string, B: string, ratio: Decimal) {
  if (graph.has(A)) {
    graph.get(A)!.push({ id: B, ratio: ratio });
  } else {
    graph.set(A, [{ id: B, ratio: ratio }]);
  }
}

const safeNumber = (value: Decimal) => {
  if (value.isNaN() || !value.isFinite()) {
    return new Decimal(0); // or new Decimal(null), depending on your database schema
  }
  const maxPrecision = 50;
  const maxScale = 18;
  const maxValue = new Decimal('9.999999999999999999999999999999999999999999999999E+31'); // Adjust based on precision and scale
  const minValue = maxValue.negated();

  if (value.greaterThan(maxValue)) {
    return maxValue;
  }
  if (value.lessThan(minValue)) {
    return minValue;
  }
  return value;
};

async function db_save_batch(events: any[], client: Client, block_creation_time: string, ETH2USD: Decimal, prod_client: Client) {
  const BATCH_SIZE = 100;

  const batches = [];
  for (let i = 0; i < events.length; i += BATCH_SIZE) {
    const batch = events.slice(i, i + BATCH_SIZE);
    batches.push(batch);
  }

  for (const batch of batches) {
    const values = [];
    const placeholders = batch.map((_, i) => {
      const offset = i * 10;
      return `($${offset + 1}, $${offset + 2}, $${offset + 3}, $${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7}, $${offset + 8}, $${offset + 9}, $${offset + 10})`;
    }).join(',');
    // console.log("placeholders",placeholders)
    // console.log("block_creation_time",block_creation_time)
    batch.forEach(event => {
      const {
        blockNumber,
        transactionHash,
        fromAddress,
        token0: { id: token0_id, amount: token0_amount},
        token1: { id: token1_id, amount: token1_amount},
      } = event;

      values.push(
        blockNumber,
        transactionHash,
        fromAddress,
        token0_id?.toLowerCase(),
        safeNumber(token0_amount ?? new Decimal(0)).toString(),
        token1_id?.toLowerCase(),
        safeNumber(token1_amount ?? new Decimal(0)).toString(),
        safeNumber(ETH2USD ?? new Decimal(0)).toString(),
        block_creation_time,
        ((new Date()).toISOString())
      );
    });
    // console.log("values",values)
    const query = `
        INSERT INTO swap_basic_events (
          block_number,
          transaction_hash,
          wallet_address,
          token0_id,
          token0_qty,
          token1_id,
          token1_qty,
          eth_price_usd,
          created_at,
          insert_timestamp
        ) VALUES ${placeholders}
      `;
    // console.log("query",query)
    // console.log("values",values)
    try {
      await client.query(query, values);
    } catch (err) {
      console.error('Error saving batch of events', err);
      fs.appendFile("./logs/error.txt", err + '\n', (err) => {
        if (err) {
          console.error('Error writing file', err);
        } else {
          console.log('File has been written successfully');
        }
      })
    }
    continue;

    try {
      await prod_client.query(query, values);
    } catch (err) {
      console.error('Error saving batch of events', err);
      fs.appendFile("./logs/prod_error.txt", err + '\n', (err) => {
        if (err) {
          console.error('Error writing file', err);
        } else {
          console.log('File has been written successfully');
        }
      })
    }
  }
}

export async function Save(swapEvents: {}[], client: Client, web3: Web3, ETH2USD: Decimal, prod_client: Client) {
  if (swapEvents.length == 0) return;
  //const result = (await client.query("SELECT * FROM ethereum_price_hist ORDER BY updated_time DESC LIMIT 1")).rows[0];
  //const ETH2USD = new Decimal(3200);
  const block_timestamp = (new Date(parseInt((await web3.eth.getBlock(swapEvents[0].blockNumber)).timestamp) * 1000)).toISOString();
  console.log(`start storing into db block ${swapEvents[0].blockNumber} at` + getCurrentTimeISOString());
  await db_save_batch(swapEvents, client, block_timestamp, ETH2USD, prod_client);


}

// Function to get the token addresses
export async function getPairTokenSymbols(web3: Web3, pairAddress: string) {
  const pairABI = [
    {
      "constant": true,
      "inputs": [],
      "name": "token0",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    },
    {
      "constant": true,
      "inputs": [],
      "name": "token1",
      "outputs": [
        {
          "internalType": "address",
          "name": "",
          "type": "address"
        }
      ],
      "payable": false,
      "stateMutability": "view",
      "type": "function"
    }
  ];
  // Create a new contract instance with the pair address and ABI
  try {
    const pairContract = new web3.eth.Contract(pairABI, pairAddress);
    const token0 = await pairContract.methods.token0().call();
    const token1 = await pairContract.methods.token1().call();
    return { token0, token1 };
  } catch (error) {
    console.error("Error fetching pair tokens:", error);
    return null;
  }
}

export interface AlchemyWebhookEvent {
  webhookId: string;
  id: string;
  createdAt: Date;
  type: AlchemyWebhookType;
  event: Record<any, any>;
}

export function getCurrentTimeISOString(): string {
  const now = new Date();
  return now.toISOString();
}

export interface PairToken {
  //pool_version: string;
  token0: string;
  token1: string;
}

export type AlchemyWebhookType =
  | "MINED_TRANSACTION"
  | "DROPPED_TRANSACTION"
  | "ADDRESS_ACTIVITY";

