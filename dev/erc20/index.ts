import { createWalletClient, http } from "viem";
import { privateKeyToAccount } from "viem/accounts";
import { foundry } from "viem/chains";

import ERC20 from "./ERC20.json";

const CONTRACT = "0x5FbDB2315678afecb367f032d93F642f64180aa3";

const [ACCOUNT_1, ACCOUNT_2] = [
  privateKeyToAccount(
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
  ),
  privateKeyToAccount(
    "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
  ),
];

const client = createWalletClient({
  chain: foundry,
  transport: http("http://pg-chainsync-foundry:8545"),
});

console.log("Transferring tokens every second!");

// Make sure anvil and the database is up
await new Promise((resolve) => setTimeout(resolve, 4000));

// Transfer tokens every second
setInterval(async () => {
  await client.writeContract({
    chain: foundry,
    address: CONTRACT,
    abi: ERC20.abi,
    functionName: "transfer",
    args: [
      ACCOUNT_2.address,
      `${Math.ceil(Math.random() * 1000)}` + "0".repeat(18),
    ],
    account: ACCOUNT_1,
  });
}, 1000);
