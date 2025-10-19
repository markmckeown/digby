use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes128Gcm, Nonce, Key 
};
use aes::cipher::generic_array::{typenum::U12};
use crate::Page;
use crate::page::PageTrait;


pub struct Aes128GcmSanity {

}

impl Aes128GcmSanity {
    pub fn encrypt_page(page: &mut Page, input_key: &Vec<u8>) {
        assert!(input_key.len() == 16, "Key is incorrect size");
        let block_size = page.block_size;
        let key = Key::<Aes128Gcm>::from_slice(input_key);
        let cipher = Aes128Gcm::new(&key);
        let nonce = Aes128Gcm::generate_nonce(&mut OsRng); // 96-bits; unique per message
        let encrypted_page_bytes = cipher.encrypt(&nonce, page.get_page_bytes()).expect("Failed to encrypt page");
        page.get_block_bytes_mut()[0 .. block_size - 12].copy_from_slice(&encrypted_page_bytes);
        page.get_block_bytes_mut()[block_size - 12 .. block_size].copy_from_slice(&nonce);
    }

    pub fn decrypt_page(page: &mut Page, input_key: &Vec<u8>) {
        assert!(input_key.len() == 16, "Key is incorrect size");
        let block_size = page.block_size;
        let key = Key::<Aes128Gcm>::from_slice(input_key);
        let cipher = Aes128Gcm::new(&key);
        let nonce: &Nonce<U12> = 
            Nonce::<U12>::from_slice(&page.get_block_bytes()[block_size - 12 .. block_size]);
        let plaintext = cipher.decrypt(nonce, 
            &page.get_block_bytes()[0 .. block_size - 12]).unwrap();
        page.get_page_bytes_mut().copy_from_slice(&plaintext);   
    }
}