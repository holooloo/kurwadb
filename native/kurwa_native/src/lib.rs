//! Native helpers for the parts of kurwadb that are pure CPU on a hot path.
//!
//! The rule for what belongs here: a narrow interface, no I/O, no awareness of
//! the cluster, and short enough never to hold a scheduler. Everything that
//! makes this a *distributed* store stays on the BEAM, where distribution,
//! supervision and a process per connection come for free - the whole
//! node-to-node protocol is under a hundred lines up there, and it would be
//! thousands down here.
//!
//! Bloom membership is the first thing to qualify. It is the read path of the
//! on-disk engine, since "is this key here" is the only question this database
//! asks, and it is a handful of shifts over a byte slice.

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a, 64 bit. Chosen over an Erlang-internal hash so a filter built by
/// either implementation reads identically in the other.
#[inline]
fn fnv1a(key: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for byte in key {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// One 64-bit hash split in half gives the two values the Kirsch-Mitzenmacher
/// combination needs, so the key is walked once however many probes there are.
#[inline]
fn probe(h1: u64, h2: u64, i: u64, bits: u64) -> u64 {
    h1.wrapping_add(i.wrapping_mul(h2)).wrapping_add(i.wrapping_mul(i)) % bits
}

#[rustler::nif]
fn bloom_fnv1a(key: rustler::Binary) -> u64 {
    fnv1a(key.as_slice())
}

/// Might `key` be in the set this filter was built from?
///
/// `false` is certain; `true` is wrong at the filter's configured rate. A
/// malformed filter answers `false` rather than raising: the caller reads that
/// as "not here, look elsewhere", which is the safe direction to be wrong in
/// when the alternative is taking down the whole VM from a NIF.
#[rustler::nif]
fn bloom_member(filter: rustler::Binary, bits: u64, hashes: u32, key: rustler::Binary) -> bool {
    let words = filter.as_slice();

    if bits == 0 || words.len() * 8 < bits as usize {
        return false;
    }

    let hash = fnv1a(key.as_slice());
    let h1 = hash & 0xffff_ffff;
    let h2 = hash >> 32;

    for i in 0..hashes as u64 {
        let position = probe(h1, h2, i, bits);
        let at = (position / 64) as usize * 8;

        let word = match words[at..at + 8].try_into() {
            Ok(bytes) => u64::from_le_bytes(bytes),
            Err(_) => return false,
        };

        if (word >> (position % 64)) & 1 == 0 {
            return false;
        }
    }

    true
}

rustler::init!("Elixir.Kurwa.Native");
