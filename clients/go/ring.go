package kurwadb

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
)

// ring is the server's consistent-hash ring (lib/kurwa/ring.ex), rebuilt bit
// for bit so the client can send a key straight to one of its replicas.
//
// A position is the first 8 bytes of SHA-256, big-endian. Each member is
// placed vnodes times, at "<node name>/<i>". A key's preference list walks
// clockwise from the first point at or after the key's position, collecting
// distinct members until it has n. The key hashed is the storage key
// (lib/kurwa/key.ex): a 0 byte then the key in the default set, or the set
// name's length, the name, then the key in a named set.
type ring struct {
	members   []string
	vnodes, n int
	positions []uint64
	owners    []string
}

func position(b []byte) uint64 {
	sum := sha256.Sum256(b)
	return binary.BigEndian.Uint64(sum[:8])
}

func newRing(members []string, vnodes, n int) *ring {
	seen := map[string]bool{}
	var uniq []string
	for _, m := range members {
		if !seen[m] {
			seen[m] = true
			uniq = append(uniq, m)
		}
	}
	sort.Strings(uniq)
	type point struct {
		pos   uint64
		owner string
	}
	points := make([]point, 0, len(uniq)*vnodes)
	for _, m := range uniq {
		for i := 0; i < vnodes; i++ {
			points = append(points, point{position([]byte(m + "/" + strconv.Itoa(i))), m})
		}
	}
	// Elixir sorts {position, node} tuples: by position, then by name.
	sort.Slice(points, func(i, j int) bool {
		if points[i].pos != points[j].pos {
			return points[i].pos < points[j].pos
		}
		return points[i].owner < points[j].owner
	})
	r := &ring{members: uniq, vnodes: vnodes, n: n, positions: make([]uint64, len(points)), owners: make([]string, len(points))}
	for i, p := range points {
		r.positions[i], r.owners[i] = p.pos, p.owner
	}
	return r
}

// preflist is the members responsible for a storage key, in preference order.
func (r *ring) preflist(storageKey []byte, n int) []string {
	total := len(r.positions)
	if total == 0 {
		return nil
	}
	want := n
	if len(r.members) < want {
		want = len(r.members)
	}
	h := position(storageKey)
	start := sort.Search(total, func(i int) bool { return r.positions[i] >= h })
	if start == total {
		start = 0
	}
	out := make([]string, 0, want)
	for step := 0; step < total && len(out) < want; step++ {
		owner := r.owners[(start+step)%total]
		dup := false
		for _, o := range out {
			if o == owner {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, owner)
		}
	}
	return out
}

func (r *ring) same(members []string, vnodes, n int) bool {
	if r == nil || r.vnodes != vnodes || r.n != n {
		return false
	}
	m := append([]string(nil), members...)
	sort.Strings(m)
	if len(m) != len(r.members) {
		return false
	}
	for i := range m {
		if m[i] != r.members[i] {
			return false
		}
	}
	return true
}

// storageKey is what the server hashes to place key in set ("" is the default set).
func storageKey(set, key string) []byte {
	if set == "" {
		return append([]byte{0}, key...)
	}
	b := make([]byte, 0, 1+len(set)+len(key))
	b = append(b, byte(len(set)))
	b = append(b, set...)
	return append(b, key...)
}
