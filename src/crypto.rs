use chacha20poly1305::aead::Aead;
use chacha20poly1305::{ChaCha20Poly1305, KeyInit, Nonce};
use prost::Message;
use sha2::{Digest, Sha256};

use crate::pb::Envelope;

const NONCE_LEN: usize = 12;

/// 基于 token 派生密钥的 ChaCha20-Poly1305 加密器.
/// token 本身不在网络上传输, server 通过尝试解密 Hello 来识别 token.
pub struct Cipher(ChaCha20Poly1305);

impl Cipher {
    pub fn from_token(token: &str) -> Self {
        let mut h = Sha256::new();
        h.update(b"c2x-v1:");
        h.update(token.as_bytes());
        let key = h.finalize();
        Cipher(ChaCha20Poly1305::new_from_slice(&key).expect("32 byte key"))
    }

    pub fn seal<M: Message>(&self, msg: &M) -> Envelope {
        let nonce_bytes: [u8; NONCE_LEN] = rand::random();
        let nonce = Nonce::from(nonce_bytes);
        let mut data = nonce_bytes.to_vec();
        let ct = self
            .0
            .encrypt(&nonce, msg.encode_to_vec().as_slice())
            .expect("encrypt");
        data.extend_from_slice(&ct);
        Envelope { data }
    }

    pub fn open<M: Message + Default>(&self, env: &Envelope) -> Option<M> {
        if env.data.len() < NONCE_LEN + 16 {
            return None;
        }
        let (n, ct) = env.data.split_at(NONCE_LEN);
        let nonce = Nonce::try_from(n).ok()?;
        let plain = self.0.decrypt(&nonce, ct).ok()?;
        M::decode(plain.as_slice()).ok()
    }
}
