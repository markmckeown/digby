// Used to compress data. They type of compression
// used is determined when the DB is created.
// Currently none and lz4 are supported.

pub struct Compressor {
    pub compressor_type: CompressorType,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum CompressorType {
    None = 0,
    LZ4 = 1,
}

impl TryFrom<u8> for CompressorType {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(CompressorType::None),
            1 => Ok(CompressorType::LZ4),
            _ => Err(()),
        }
    }
}

impl From<CompressorType> for u8 {
    fn from(value: CompressorType) -> Self {
        match value {
            CompressorType::None => 0,
            CompressorType::LZ4 => 1,
        }
    }
}

impl Compressor {
    pub fn new(compressor_type: CompressorType) -> Self {
        Compressor { compressor_type }
    }

    pub fn compress(&self, data: &[u8]) -> Vec<u8> {
        match self.compressor_type {
            CompressorType::None => data.to_vec(),
            CompressorType::LZ4 => lz4_flex::compress_prepend_size(data),
        }
    }

    pub fn decompress(&self, data: &[u8]) -> Vec<u8> {
        match self.compressor_type {
            CompressorType::None => data.to_vec(),
            CompressorType::LZ4 => lz4_flex::decompress_size_prepended(data).unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressor_type_try_from() {
        assert_eq!(CompressorType::try_from(0).unwrap(), CompressorType::None);
        assert_eq!(CompressorType::try_from(1).unwrap(), CompressorType::LZ4);
        assert!(CompressorType::try_from(2).is_err());
    }

    #[test]
    fn test_compressor_type_from() {
        assert_eq!(u8::from(CompressorType::None), 0);
        assert_eq!(u8::from(CompressorType::LZ4), 1);
    }

    #[test]
    fn test_compressor_none() {
        let compressor = Compressor::new(CompressorType::None);
        let data = b"hello world";

        let compressed = compressor.compress(data);
        assert_eq!(compressed, data);

        let decompressed = compressor.decompress(&compressed);
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compressor_lz4() {
        let compressor = Compressor::new(CompressorType::LZ4);
        // Create some highly compressible data
        let data = vec![b'a'; 1000];

        let compressed = compressor.compress(&data);
        // Compressed data should be smaller than the original data
        assert!(compressed.len() < data.len());

        let decompressed = compressor.decompress(&compressed);
        assert_eq!(decompressed, data);
    }
}
