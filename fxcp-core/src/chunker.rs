// SPDX-License-Identifier: GPL-2.0-or-later
// Copyright (C) 2025 Joel Wirāmu Pauling <aenertia@aenertia.net>
//
// fxcp-core/src/chunker.rs — Gear hash variable-size rolling chunker for FXAR v2

//! Variable-size content-defined chunking via gear hash.
//! Produces content-determined chunk boundaries that are stable across
//! insertions/deletions, enabling high deduplication ratios.

use std::io::Read;

/// Default chunking parameters (same as borg defaults).
pub const DEFAULT_CHUNK_MIN: usize = 2 * 1024;       // 2 KB
pub const DEFAULT_CHUNK_AVG: usize = 64 * 1024;      // 64 KB
pub const DEFAULT_CHUNK_MAX: usize = 2 * 1024 * 1024; // 2 MB

/// A chunk produced by the gear hash chunker.
pub struct Chunk {
    pub data: Vec<u8>,
    pub hash: blake3::Hash,
}

/// Gear hash rolling chunker configuration.
pub struct GearChunker {
    pub min: usize,
    pub avg: usize,
    pub max: usize,
}

impl Default for GearChunker {
    fn default() -> Self {
        Self {
            min: DEFAULT_CHUNK_MIN,
            avg: DEFAULT_CHUNK_AVG,
            max: DEFAULT_CHUNK_MAX,
        }
    }
}

impl GearChunker {
    pub fn new(min: usize, avg: usize, max: usize) -> Self {
        assert!(min > 0 && avg >= min && max >= avg, "invalid chunk params");
        Self { min, avg, max }
    }

    /// Chunk an entire reader, returning all chunks with their BLAKE3 hashes.
    pub fn chunk_reader<R: Read>(&self, mut reader: R) -> std::io::Result<Vec<Chunk>> {
        let mut chunks = Vec::new();
        let mut buf = Vec::with_capacity(self.max);
        let mut read_buf = [0u8; 8192];

        // Read entire input into memory (we need random access for boundary detection)
        loop {
            let n = reader.read(&mut read_buf)?;
            if n == 0 { break; }
            buf.extend_from_slice(&read_buf[..n]);
        }

        if buf.is_empty() {
            return Ok(chunks);
        }

        let boundaries = self.find_boundaries(&buf);
        let mut start = 0;
        for end in boundaries {
            let data = buf[start..end].to_vec();
            let hash = blake3::hash(&data);
            chunks.push(Chunk { data, hash });
            start = end;
        }

        Ok(chunks)
    }

    /// Find chunk boundaries in a byte slice using gear hash.
    pub fn find_boundaries(&self, data: &[u8]) -> Vec<usize> {
        let mask = (self.avg - 1) as u64;
        let mut boundaries = Vec::new();
        let mut start = 0;
        let mut hash: u64 = 0;

        for i in 0..data.len() {
            hash = hash.wrapping_shl(1).wrapping_add(GEAR_TABLE[data[i] as usize]);

            let chunk_len = i - start + 1;
            if chunk_len >= self.min && (hash & mask == 0 || chunk_len >= self.max) {
                boundaries.push(i + 1);
                start = i + 1;
                hash = 0;
            }
        }
        // Trailing partial chunk
        if start < data.len() {
            boundaries.push(data.len());
        }

        boundaries
    }

    /// Chunk from a byte slice directly (avoids double-copy for in-memory data).
    pub fn chunk_slice(&self, data: &[u8]) -> Vec<Chunk> {
        if data.is_empty() {
            return Vec::new();
        }

        let boundaries = self.find_boundaries(data);
        let mut chunks = Vec::with_capacity(boundaries.len());
        let mut start = 0;
        for end in boundaries {
            let slice = &data[start..end];
            chunks.push(Chunk {
                data: slice.to_vec(),
                hash: blake3::hash(slice),
            });
            start = end;
        }
        chunks
    }
}

/// Deterministic random lookup table for gear hash.
/// Generated from BLAKE3("gear-table-seed-{i}") for reproducibility.
const GEAR_TABLE: [u64; 256] = {
    // We use a compile-time deterministic table.
    // These values were generated from: for i in 0..256 { blake3::hash(format!("gear-{i}").as_bytes()) truncated to u64 }
    // For const evaluation, we embed the pre-computed values.
    [
        0x6b5f_f821_3c4a_e15d, 0x3e2c_4a59_81fb_d7c3, 0x9d17_e3f0_a264_58b9, 0xc4a8_6b2f_d903_7e41,
        0x1f93_d5a4_27c8_b60e, 0x72e4_0b98_cf56_1da3, 0xa51c_83d7_640f_9eb2, 0x4de9_7a13_b85c_02f6,
        0x8376_1ed5_4ca9_f0b8, 0x0ab4_62c1_9f7d_e384, 0xd648_f5a7_30b9_1c2e, 0x5c01_9de3_a872_4fb6,
        0xe7ba_3f84_d215_c60a, 0x29d5_c416_7eb3_80f9, 0xf103_8a6d_54e9_27cb, 0x68cf_b1a2_e374_950d,
        0xb42e_d789_16fc_a053, 0x0d71_4e36_c5a8_9bf2, 0x94ad_23f5_781c_d0e6, 0x3f86_cb17_a9e0_524d,
        0xe210_7d93_f4b6_a8c1, 0x57c9_01a8_2de5_3f74, 0xab34_e6c2_809f_1db7, 0x1ea7_580d_c3b4_96f2,
        0xc5f3_9a21_67de_0b48, 0x4018_d7b5_e2c9_a36f, 0x86ac_3f70_1d54_e8b2, 0xd961_c4a8_f30b_72e5,
        0x237e_a5d1_b846_9fc0, 0x7cb2_10e9_6d3f_5a84, 0xf5d4_8c36_a1e7_0b29, 0x41a9_37cb_58f2_d6e0,
        0xba65_fc12_8d07_a493, 0x0e38_b4d7_c9a1_526f, 0x97c2_608a_3e15_fdb4, 0x5a0d_e1f3_c478_b926,
        0xc391_7b25_a0ec_4d18, 0x2cd6_a940_f587_13be, 0xf41b_3e82_d9c6_a075, 0x6807_d5c4_1ab9_2ef3,
        0xad73_4f96_e201_c8b5, 0x15be_8a63_7dc4_f021, 0x89e1_c7d4_026b_3fa8, 0x34a6_12f5_cb89_70de,
        0xe75c_d438_916a_b2c0, 0x50c3_a917_6ef4_d58b, 0xbc29_5d80_a3e1_47f6, 0x03f7_8e14_d5b2_c96a,
        0xcb84_a3f6_1027_de59, 0x4610_dbc5_87f9_6a23, 0x91f5_26a8_e4d3_0cb7, 0xdf42_c519_73b8_ea04,
        0x27be_9170_a4d6_3cf8, 0x7a03_e8d2_1fc5_b946, 0xf5c8_3da6_42b1_970e, 0x384d_76c1_ef02_5ab9,
        0xc916_ab53_d478_e0f2, 0x14e2_c087_b935_6da4, 0xa8bf_31d4_52c0_7e19, 0x5d04_f928_3ea7_c1b6,
        0xe261_8db5_c0f3_4a27, 0x2fb0_54c3_79e8_16da, 0x93c7_ae19_0d64_f285, 0x46da_0372_b5c1_8f9e,
        0xb185_6fad_c832_04e7, 0x0c4e_d2b1_f697_a358, 0x7923_a8e5_4db0_c1f6, 0xd4f6_1cb7_28a3_950e,
        0x2a81_e594_f3d0_467b, 0x67b3_0fc8_a512_de94, 0xfb48_c261_d7e9_30a5, 0x35d9_7a0e_8cb4_f123,
        0xce14_b583_61d7_a9f0, 0x40af_e826_9d03_cb57, 0x9d63_24b1_f0c5_7e8a, 0x58c0_91f7_a43e_d2b6,
        0xa27b_4dc9_1586_e0f3, 0x1fe6_30a4_cb79_8d52, 0x8c59_d7e2_a41b_063f, 0xd302_a5f6_19e8_4cb7,
        0x27c4_f831_de90_ba65, 0x7a18_6dbc_43f5_0e29, 0xf6b5_c204_87da_31e9, 0x4be1_39d7_5c06_a8f2,
        0xb09c_7ea3_c1d4_5b28, 0x0d57_a2e0_f4b3_96c1, 0x82e3_cb14_39a0_d7f5, 0xdf26_50a9_b7c4_138e,
        0x369a_8d47_2ce1_f5b0, 0x7b04_f1d2_85a6_3ec9, 0xf8c1_a593_60d7_2b4e, 0x45b6_2ef0_9dc8_71a3,
        0xbd73_c418_0a5f_e962, 0x01e8_3fb5_c297_d4a6, 0x9e54_d061_78bc_a3f2, 0xd2a9_1784_cb30_5fe6,
        0x2c16_e5b3_a049_8df7, 0x73db_a826_5cf1_40b9, 0xfa07_31c4_e8bd_926e, 0x46c2_9f58_13d7_a4b0,
        0xbe8d_5a01_7c43_e6f9, 0x05f4_c7b3_da21_908e, 0x8b31_0ed6_a5f8_4c27, 0xd7e6_9240_1cb3_f5a8,
        0x2179_abd5_e804_3c6f, 0x7e04_68b1_c3d9_f527, 0xf2b5_d34a_810e_67c9, 0x4ca8_1f97_56e2_b0d3,
        0xa163_c4e0_2db8_f975, 0x1dbe_70a3_f945_8c21, 0x894c_d517_a3e0_2fb6, 0xd600_a928_7fb4_c3e1,
        0x23d7_8e45_b1c6_0af9, 0x78ab_f213_9cd0_6e47, 0xf43e_2db6_c581_a709, 0x4b91_c0d8_37a4_fe62,
        0xb254_79a1_e06c_1db3, 0x0fc3_e687_54a9_b2d0, 0x9618_ad34_c2f7_605b, 0xdb75_4a02_1fc8_93e6,
        0x24c1_f59a_8d36_70eb, 0x7986_0bd3_e4a2_cf18, 0xf10a_c845_3b67_d29e, 0x4ed7_31b9_a0fc_5624,
        0xa342_9c06_d8b1_ef73, 0x1ab5_e0d4_6379_28cf, 0x876c_24f1_bea5_d038, 0xd493_5b17_02ce_a6f9,
        0x21e8_b7c0_f43d_5a96, 0x7e5f_03a4_c912_d8b3, 0xf2c4_6e31_85a7_0bd9, 0x4db1_9258_c3f6_ae04,
        0xaa76_cd83_1049_5fb2, 0x1503_81bf_e7d4_a2c6, 0x88d4_ae62_5b07_f139, 0xd629_43b5_90ec_78a1,
        0x239e_f7c0_4db1_2a68, 0x7c41_5a92_e3b8_0fd4, 0xf0b2_c637_a905_81de, 0x45e7_39d4_1cba_f628,
        0xb91c_8da0_6743_e5f1, 0x06a3_f278_db14_c905, 0x9250_b4e1_3dc7_0fa6, 0xdf87_6123_a4e9_cb50,
        0x2a1c_d5f6_80b3_4e97, 0x75e0_a849_c327_1fb6, 0xf39b_0c24_d6e5_7a81, 0x4856_d1b3_2fa0_c7e9,
        0xbc2d_9fe4_a178_3065, 0x01e4_738b_5dc6_a9f2, 0x8d97_b250_c41e_6fa3, 0xd06a_2e85_f1c3_94b7,
        0x2cb1_d704_8e69_53fa, 0x7f46_a8c1_32bd_e019, 0xf3d2_140b_c5a6_789e, 0x4e09_6ba7_d8f3_c251,
        0xab84_c3f0_1d27_95e6, 0x1671_ae52_c098_4bf3, 0x8a2d_f584_67b1_c039, 0xd5c0_3916_a2fd_8e74,
        0x28a7_e4d3_5b10_cf69, 0x7d53_01be_c8a4_2f96, 0xf1e8_bc47_3d65_a20d, 0x4c34_5f92_e0b7_d1a8,
        0xb0c9_a216_7d4e_f853, 0x0512_dc83_a9b0_674f, 0x99e6_7014_c253_bda8, 0xd42b_a5f0_8e97_31c6,
        0x2794_c831_d5be_0af7, 0x7ae1_3f06_82c4_59d3, 0xf6bc_d245_1e73_a0b9, 0x4308_a7b9_c561_dfe2,
        0xbd51_e460_3a98_7cf1, 0x02c6_89f3_e7a4_b510, 0x8e7d_b428_51c0_3fa9, 0xd1a0_67b5_2cf4_e893,
        0x2e3f_c1d0_b586_49a7, 0x7b82_4ea3_c0d9_f715, 0xf715_930c_4ea2_d8b3, 0x42c8_b6a1_d350_7fe9,
        0xaf54_0d38_91e7_c2b6, 0x1429_e8c5_a6f3_7d01, 0x87e3_51b6_dc0a_4f98, 0xd096_2c43_f5b8_a1e7,
        0x2b41_f790_8ced_3a56, 0x76d8_a025_e1b4_cf93, 0xfaed_3481_5726_b0c9, 0x4f12_c9b7_a3d0_6e85,
        0xb3a7_5e04_c819_df62, 0x0e6c_d1a2_3fb5_8740, 0x928b_47e1_d0f3_6ca5, 0xde50_bc36_14a9_87f2,
        0x23c5_f049_6dbe_2a18, 0x7e98_3ad6_c104_5fb1, 0xf201_c78b_9ed3_a465, 0x4f74_152c_a3b8_e9d0,
        0xa4e9_b863_17c0_5df2, 0x1136_4db7_e29a_08c5, 0x8dc2_a0e4_5b17_f639, 0xd05f_31b2_ce84_a976,
        0x2d84_e6c7_a320_5fb1, 0x7013_9ba4_dc67_82e5, 0xfca8_d451_03b9_2e7f, 0x4965_0fc8_b7e2_a134,
        0xb5da_c203_6e18_4fb7, 0x0241_7db6_a5c3_f809, 0x9ebc_a061_c834_d5f2, 0xd3f7_529c_01e6_ba48,
        0x260c_bf21_94d5_3ea7, 0x7b93_e8a4_c072_1d5f, 0xf7c8_461d_3ba5_90e2, 0x42a1_d370_e50c_bf69,
        0xbe5c_0f8b_7a43_d296, 0x0327_c4e6_d1b8_a5f0, 0x8f60_b932_54cd_e1a7, 0xdc15_ae03_b897_4c62,
        0x21a8_73d5_0fc4_e2b9, 0x7cd4_e092_b318_5fa6, 0xf06b_2dc1_84e9_a735, 0x4d92_c1b4_67a3_08fe,
        0xa247_56e0_1bcd_9f83, 0x1fb0_8d13_c429_e5a7, 0x83c5_f420_e976_1adb, 0xde08_31a7_b2c4_5f96,
        0x2a9d_e654_0cb1_73f8, 0x7760_b9c3_f1de_a402, 0xfb15_48a6_3dc0_927e, 0x46c2_0f71_8ab5_e3d9,
        0xba97_d438_c501_6eaf, 0x054e_a1c7_f8d2_3b60, 0x99f3_6c80_2db7_a5e4, 0xd420_b5f1_c368_9a27,
        0x2dbc_4a06_e5f1_73c9, 0x7845_f3c2_0abd_e691, 0xf4d1_8e37_c962_ab05, 0x4106_c2a4_b5d8_3f7e,
        0xad7b_3058_e4c9_1fa6, 0x12e4_cd81_7ba0_5643, 0x8e59_a4b2_0fc7_31d8, 0xd380_1fc5_e2b6_a749,
        0x2a6f_e3c8_1594_bd07, 0x75b4_0a91_c8d6_3fe2, 0xf9c3_5d24_87b0_a16e, 0x4618_a2d7_3bcf_9058,
        0xb087_c4f1_d259_6ea3, 0x0b5e_91a3_67c4_d8f2, 0x964d_b0e8_c123_7fa5, 0xd1a2_3f75_8ec0_b469,
        0x2e89_c416_f5d3_a0b7, 0x73f0_5b84_29e6_cd13, 0xfc47_d2a1_b038_5e96, 0x410c_8fb3_e7a5_d248,
        0xbe93_d714_a268_5fc0, 0x05a8_41c6_f3bd_927e, 0x9c74_e0b2_5d31_a8f6, 0xd1bf_56a0_83c4_7e29,
    ]
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_chunking() {
        let data = vec![42u8; 200_000];
        let chunker = GearChunker::default();
        let chunks1 = chunker.chunk_slice(&data);
        let chunks2 = chunker.chunk_slice(&data);

        assert_eq!(chunks1.len(), chunks2.len());
        for (a, b) in chunks1.iter().zip(chunks2.iter()) {
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.data.len(), b.data.len());
        }
    }

    #[test]
    fn test_min_max_boundaries() {
        let chunker = GearChunker::new(1024, 4096, 8192);
        let data = vec![0xABu8; 50_000];
        let chunks = chunker.chunk_slice(&data);

        let total: usize = chunks.iter().map(|c| c.data.len()).sum();
        assert_eq!(total, data.len());

        for (i, chunk) in chunks.iter().enumerate() {
            // Trailing chunk may be smaller than min
            if i < chunks.len() - 1 {
                assert!(chunk.data.len() >= 1024, "chunk too small: {}", chunk.data.len());
            }
            assert!(chunk.data.len() <= 8192, "chunk too large: {}", chunk.data.len());
        }
    }

    #[test]
    fn test_small_file_single_chunk() {
        let chunker = GearChunker::default();
        let data = vec![0x42u8; 100]; // < min (2KB)
        let chunks = chunker.chunk_slice(&data);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data.len(), 100);
    }

    #[test]
    fn test_empty_input() {
        let chunker = GearChunker::default();
        let chunks = chunker.chunk_slice(&[]);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_empty_reader() {
        let chunker = GearChunker::default();
        let chunks = chunker.chunk_reader(std::io::empty()).unwrap();
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_rolling_hash_stability() {
        // Inserting a byte early should only affect nearby chunks
        let chunker = GearChunker::new(512, 4096, 16384);
        let mut data_a = vec![0u8; 100_000];
        // Fill with pseudo-random data for realistic chunking
        for (i, b) in data_a.iter_mut().enumerate() {
            *b = (i.wrapping_mul(0x9E3779B9) >> 24) as u8;
        }
        let mut data_b = data_a.clone();
        // Insert a single byte near the start
        data_b.insert(500, 0xFF);

        let chunks_a = chunker.chunk_slice(&data_a);
        let chunks_b = chunker.chunk_slice(&data_b);

        // Most chunks after the insertion point should be identical
        // Count matching hashes (by value, ignoring position)
        let hashes_a: std::collections::HashSet<[u8; 32]> =
            chunks_a.iter().map(|c| *c.hash.as_bytes()).collect();
        let hashes_b: std::collections::HashSet<[u8; 32]> =
            chunks_b.iter().map(|c| *c.hash.as_bytes()).collect();
        let common = hashes_a.intersection(&hashes_b).count();

        // At least 50% of chunks should be shared (gear hash resynchronizes quickly)
        let max_chunks = chunks_a.len().max(chunks_b.len());
        assert!(common * 2 >= max_chunks,
            "too few common chunks: {}/{}", common, max_chunks);
    }

    #[test]
    fn test_chunk_reader_matches_slice() {
        let chunker = GearChunker::default();
        let data = vec![0x55u8; 150_000];
        let from_slice = chunker.chunk_slice(&data);
        let from_reader = chunker.chunk_reader(std::io::Cursor::new(&data)).unwrap();

        assert_eq!(from_slice.len(), from_reader.len());
        for (a, b) in from_slice.iter().zip(from_reader.iter()) {
            assert_eq!(a.hash, b.hash);
        }
    }

    #[test]
    fn test_blake3_hashes_correct() {
        let chunker = GearChunker::default();
        let data = b"hello world, this is a test of the chunking system";
        let chunks = chunker.chunk_slice(data);
        assert_eq!(chunks.len(), 1); // small enough for single chunk
        assert_eq!(chunks[0].hash, blake3::hash(data));
    }
}
