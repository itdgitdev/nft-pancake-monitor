const { Connection, VersionedMessage, VersionedTransaction, clusterApiUrl, Keypair } = solanaWeb3;

// export async function signAndSend(msg_base64) {
//   try {
//     // 1️⃣ Giải mã message từ base64
//     const messageBytes = Uint8Array.from(atob(msg_base64), (c) => c.charCodeAt(0));

//     // 2️⃣ Deserialize message
//     const msg = VersionedMessage.deserialize(messageBytes);

//     // 3️⃣ Tạo transaction từ message (chưa ký)
//     const tx = new VersionedTransaction(msg);

//     // 4️⃣ Yêu cầu Phantom ký transaction
//     if (!window.solana) throw new Error("❌ Phantom wallet not found");
//     const signedTx = await window.solana.signTransaction(tx);

//     // 5️⃣ Kết nối RPC node
//     const connection = new solanaWeb3.Connection(
//         "https://dawn-blissful-pallet.solana-mainnet.quiknode.pro/a2995d002f97f0eb9165a1d8ce906d2ce626aa85/",
//         "confirmed"
//     );

//     // 6️⃣ Mô phỏng transaction trước khi gửi
//     const sim = await connection.simulateTransaction(signedTx, {
//       sigVerify: false,
//       commitment: "processed",
//     });

//     console.log("🧩 Simulation logs:", sim.value.logs);
//     if (sim.value.err) {
//       console.error("❌ Simulation error:", sim.value.err);
//       console.warn("⚠️ Transaction bị revert — không nên gửi lên mạng thật");
//       return;
//     }

//     // 7️⃣ Nếu simulation OK, gửi transaction thật
//     const txid = await connection.sendRawTransaction(signedTx.serialize(), {
//       skipPreflight: false,
//       maxRetries: 3,
//     });

//     console.log("✅ Transaction sent successfully!");
//     console.log("🔗 Explorer:", `https://solscan.io/tx/${txid}`);

//     return txid;

//   } catch (e) {
//     console.error("❌ signAndSend error:", e);
//   }
// }

export async function signAndSend(msg_base64, position_nft_mint_secret) {
    if (!window.solana) throw new Error("❌ Phantom not found");
    
    try {
        // 1. Deserialize message
        const msgBytes = Uint8Array.from(atob(msg_base64), c => c.charCodeAt(0));
        const msg = VersionedMessage.deserialize(msgBytes);
        
        // 2. Recreate position_nft_mint keypair from secret
        const positionNftMintKp = Keypair.fromSecretKey(
            new Uint8Array(position_nft_mint_secret)
        );
        
        console.log("NFT mint pubkey:", positionNftMintKp.publicKey.toBase58());
        
        // 3. Create transaction
        const tx = new VersionedTransaction(msg);
        
        // 4. Sign với position_nft_mint keypair
        tx.sign([positionNftMintKp]);
        
        console.log("✅ Signed with NFT mint");
        
        // 5. Phantom sign với payer
        const signedTx = await window.solana.signTransaction(tx);
        
        console.log("✅ Signed with Phantom (payer)");
        
        // 6. Simulate
        const connection = new solanaWeb3.Connection(
            "https://dawn-blissful-pallet.solana-mainnet.quiknode.pro/a2995d002f97f0eb9165a1d8ce906d2ce626aa85/",
            "confirmed"
        );
        
        const sim = await connection.simulateTransaction(signedTx, {
            sigVerify: true,
            commitment: "processed"
        });
        
        console.log("Simulation:", sim.value);
        
        if (sim.value.err) {
            console.error("❌ Simulation failed:", sim.value.err);
            console.log("Logs:", sim.value.logs);
            return;
        }
        
        // 7. Send
        const txid = await connection.sendRawTransaction(signedTx.serialize());
        console.log("✅ Tx:", `https://solscan.io/tx/${txid}`);
        return txid;
        
    } catch (e) {
        console.error("❌ Error:", e);
        console.error("Stack:", e.stack);
    }
}
