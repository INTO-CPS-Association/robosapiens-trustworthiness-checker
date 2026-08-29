//! Deterministic non-cryptographic fingerprints for compiled semantic identities.

const OFFSET_A: u64 = 0xcbf2_9ce4_8422_2325;
const OFFSET_B: u64 = 0x8422_2325_cbf2_9ce4;
const PRIME_A: u64 = 0x0000_0100_0000_01b3;
const PRIME_B: u64 = 0x9e37_79b1_85eb_ca87;

/// Incrementally constructs a stable 128-bit fingerprint.
///
/// Each value is length-delimited so callers can compose keys without allocating a canonical
/// serialization. This is deliberately non-cryptographic: configuration and monitor definitions
/// are trusted, and the key is only an efficient equality discriminator.
pub(crate) struct FingerprintBuilder {
    a: u64,
    b: u64,
}

impl FingerprintBuilder {
    pub(crate) fn new(domain: &'static str) -> Self {
        let mut builder = Self {
            a: OFFSET_A,
            b: OFFSET_B,
        };
        builder.write_bytes(domain.as_bytes());
        builder
    }

    pub(crate) fn write_str(&mut self, value: &str) {
        self.write_bytes(value.as_bytes());
    }

    pub(crate) fn write_u128(&mut self, value: u128) {
        self.write_bytes(&value.to_le_bytes());
    }

    pub(crate) fn write_bytes(&mut self, value: &[u8]) {
        self.mix_u64(value.len() as u64);
        for &byte in value {
            self.a ^= u64::from(byte);
            self.a = self.a.wrapping_mul(PRIME_A);

            self.b ^= u64::from(byte).wrapping_add(self.a.rotate_left(17));
            self.b = self.b.wrapping_mul(PRIME_B).rotate_left(11);
        }
    }

    pub(crate) fn finish(self) -> u128 {
        let a = avalanche(self.a ^ self.b.rotate_left(23));
        let b = avalanche(self.b ^ self.a.rotate_right(19));
        (u128::from(a) << 64) | u128::from(b)
    }

    fn mix_u64(&mut self, value: u64) {
        for byte in value.to_le_bytes() {
            self.a ^= u64::from(byte);
            self.a = self.a.wrapping_mul(PRIME_A);
            self.b ^= u64::from(byte).wrapping_add(self.a.rotate_left(17));
            self.b = self.b.wrapping_mul(PRIME_B).rotate_left(11);
        }
    }
}

pub(crate) fn fingerprint(domain: &'static str, value: &[u8]) -> u128 {
    let mut builder = FingerprintBuilder::new(domain);
    builder.write_bytes(value);
    builder.finish()
}

fn avalanche(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51_afd7_ed55_8ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    value ^ (value >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprints_are_deterministic_and_domain_separated() {
        assert_eq!(fingerprint("stream", b"x"), fingerprint("stream", b"x"));
        assert_ne!(fingerprint("stream", b"x"), fingerprint("stream", b"y"));
        assert_ne!(fingerprint("stream", b"x"), fingerprint("definition", b"x"));
    }

    #[test]
    fn composed_values_are_length_delimited() {
        let mut left = FingerprintBuilder::new("test");
        left.write_str("ab");
        left.write_str("c");

        let mut right = FingerprintBuilder::new("test");
        right.write_str("a");
        right.write_str("bc");

        assert_ne!(left.finish(), right.finish());
    }
}
