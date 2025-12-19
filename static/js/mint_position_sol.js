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

const RPC_URL = "https://dawn-blissful-pallet.solana-mainnet.quiknode.pro/a2995d002f97f0eb9165a1d8ce906d2ce626aa85/";
export async function signAndSend(msg_base64, position_nft_mint_secret) {
    if (!window.solana) throw new Error("❌ Phantom not found");

    try {
        // Deserialize message
        const msgBytes = Uint8Array.from(atob(msg_base64), c => c.charCodeAt(0));
        const msg = VersionedMessage.deserialize(msgBytes);
        let tx = new VersionedTransaction(msg);

        const phantom = window.solana;

        // 1) Phantom ký TRƯỚC
        tx = await phantom.signTransaction(tx);

        // 2) Thêm chữ ký của NFT mint
        const mintKp = Keypair.fromSecretKey(
            new Uint8Array(position_nft_mint_secret)
        );

        tx.sign([mintKp]); // ĐÚNG: ký vào tx đã được phantom ký

        // 3) Simulate đúng
        const connection = new solanaWeb3.Connection(RPC_URL, "processed");
        const sim = await connection.simulateTransaction(tx, {
            sigVerify: false,
            replaceRecentBlockhash: true
        });

        if (sim.value.err) {
            console.error("❌ Simulation error:", sim.value.err);
            return;
        }

        // 4) Send đúng
        const sig = await connection.sendRawTransaction(tx.serialize(), {
            skipPreflight: false
        });

        console.log("✅ Tx sent:", sig);
        return sig;

    } catch (e) {
        console.error("❌ Error:", e);
    }
}

export async function signAndSendTxBase64(tx_base64) {
    if (!window.solana) throw new Error("❌ Phantom not found");
    const phantom = window.solana;
    const connection = new Connection(RPC_URL, "confirmed");

    // 1. Deserialize VersionedTransaction từ backend
    const txBytes = Uint8Array.from(atob(tx_base64), (c) => c.charCodeAt(0));
    let tx = VersionedTransaction.deserialize(txBytes);

    console.log("📄 Tx from backend:", tx);

    // 2. Phantom ký cho payer (ví user)
    // LƯU Ý: backend đã ký sẵn cho NFT mint.
    // Phantom sẽ thêm/chèn chữ ký cho account tương ứng với publicKey của ví.
    tx = await phantom.signTransaction(tx);

    console.log("✅ After Phantom sign:", tx);

    // 3. (optional) simulate trước khi gửi
    const sim = await connection.simulateTransaction(tx, {
        sigVerify: true,
        commitment: "processed",
    });

    if (sim.value.err) {
        console.log("❌ Simulation error:", sim.value)
        console.error("❌ Simulation error:", sim.value.err);
        throw new Error("Simulation failed, không nên gửi tx này");
    }

    // 4. Gửi tx lên mạng
    const sig = await connection.sendRawTransaction(tx.serialize(), {
        skipPreflight: false,
    });

    console.log("✅ Tx sent:", sig);
    console.log("🔗 https://solscan.io/tx/" + sig);

    const confirmation = await connection.confirmTransaction(sig, "confirmed");
    console.log("📦 Confirmed:", confirmation);

    return sig;
}

export async function getTokenBalance(mint) {
    const connection = new solanaWeb3.Connection(RPC_URL, "confirmed");
    
    // MUST ADD await !!!
    const resp = await window.solana.connect();
    const wallet = resp.publicKey.toString();
    console.log("👤 User:", wallet);

    if (!wallet || !mint) return 0;

    const owner = new solanaWeb3.PublicKey(wallet);
    const mint_pubkey = new solanaWeb3.PublicKey(mint);

    if (mint === "So11111111111111111111111111111111111111112") {
        const lamports = await connection.getBalance(owner);
        return lamports / solanaWeb3.LAMPORTS_PER_SOL;
    }

    const res = await connection.getParsedTokenAccountsByOwner(
        owner,
        { mint: mint_pubkey }
    );

    if (res.value.length === 0) return 0;

    return res.value[0].account.data.parsed.info.tokenAmount.uiAmount;
}
