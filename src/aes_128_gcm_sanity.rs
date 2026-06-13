use crate::Page;
use crate::page::PageTrait;
use aes::cipher::generic_array::typenum::U12;
use aes_gcm::{
    Aes128Gcm, Key, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};

// Support for encrypting blocks.
// Each block is encrypted with a randomly generated 96 bit nonce
// and the key provided which is 16 bytes.
// A nonce is generated each time a block is encrypted.
// The nonce is stored in the last 12 bytes of the block. An additional
// 16 bytes are used for the AES-128-GCM authentication tag, so the
// page size is block size - 28.
// There is no checksum stored, AES128-GCM has a built in
// cryptographic checksum functionality.
pub struct Aes128GcmSanity {}

impl Aes128GcmSanity {
    pub fn encrypt_page(page: &mut Page, input_key: &Vec<u8>) {
        assert!(input_key.len() == 16, "Key is incorrect size");
        let block_size = page.block_size;
        let key: &Key<Aes128Gcm> = input_key.as_slice().into();
        let cipher = Aes128Gcm::new(key);
        let nonce = Aes128Gcm::generate_nonce(&mut OsRng); // 96-bits; unique per run.
        // The encrypted size matches the unencrypted size.
        let encrypted_page_bytes = cipher
            .encrypt(&nonce, page.get_page_bytes())
            .expect("Failed to encrypt page");
        // Copy the encrypted bytes back into the page followed by the nonce.
        page.get_block_bytes_mut()[0..block_size - 12].copy_from_slice(&encrypted_page_bytes);
        page.get_block_bytes_mut()[block_size - 12..block_size].copy_from_slice(&nonce);
    }

    pub fn decrypt_page(page: &mut Page, input_key: &Vec<u8>) {
        assert!(input_key.len() == 16, "Key is incorrect size");
        let block_size = page.block_size;
        let key: &Key<Aes128Gcm> = input_key.as_slice().into();
        let cipher = Aes128Gcm::new(key);
        let nonce: &Nonce<U12> = (&page.get_block_bytes()[block_size - 12..block_size]).into();
        let plaintext = cipher.decrypt(nonce, &page.get_block_bytes()[0..block_size - 12]);
        let mut plaintext = plaintext.expect("Failed to decrypt page");
        // Pad the plaintext to the block size if necessary
        plaintext.resize(page.get_block_bytes().len(), 0);
        // Copy the unencrypted bytes back into the page.
        page.replace_bytes(plaintext);
    }
}
