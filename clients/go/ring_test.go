package kurwadb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

// clients/ring_fixture.json is written by the server's own Kurwa.Ring
// (clients/ring_fixture.exs); the client must reproduce it bit for bit.
func TestRingMatchesServerFixture(t *testing.T) {
	raw, err := os.ReadFile("../ring_fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Cases []struct {
			Members  []string `json:"members"`
			Vnodes   int      `json:"vnodes"`
			N        int      `json:"n"`
			Set      string   `json:"set"`
			Key      string   `json:"key"`
			Preflist []string `json:"preflist"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Cases) < 500 {
		t.Fatalf("only %d cases", len(f.Cases))
	}
	rings := map[string]*ring{}
	for _, c := range f.Cases {
		id := fmt.Sprintf("%s|%d|%d", strings.Join(c.Members, ","), c.Vnodes, c.N)
		r, ok := rings[id]
		if !ok {
			r = newRing(c.Members, c.Vnodes, c.N)
			rings[id] = r
		}
		got := r.preflist(storageKey(c.Set, c.Key), c.N)
		if len(got) == 0 && len(c.Preflist) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, c.Preflist) {
			t.Fatalf("%s/%q: got %v, want %v", c.Set, c.Key, got, c.Preflist)
		}
	}
}

// The live server's own preference lists, against the client's.
func TestRoutingAgreesWithServer(t *testing.T) {
	ctx := context.Background()
	if db.currentRing() == nil {
		t.Fatal("the server did not answer KURWA.RING")
	}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("route:%d", i)
		set := ""
		if i%2 == 0 {
			set = "seen"
		}
		r, err := db.run(ctx, [][]string{{"KURWA.PREFLIST", set, key}}, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		list, _ := r[0].([]any)
		want := make([]string, len(list))
		for j, v := range list {
			want[j] = fmt.Sprint(v)
		}
		if got := db.Replicas(key, set); !reflect.DeepEqual(got, want) {
			t.Fatalf("%s/%s: client %v, server %v", set, key, got, want)
		}
	}
}

func TestRoutingOffAndSplitHasMany(t *testing.T) {
	ctx := context.Background()
	off, err := Connect(ctx, Options{Nodes: seeds, Password: password, NoRouting: true})
	if err != nil {
		t.Fatal(err)
	}
	defer off.Close()
	keys := make([]string, 50)
	want := make([]bool, 50)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s:%d", id(), i)
		if i < 25 {
			want[i] = true
			if err := db.Add(ctx, keys[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, c := range []*Client{db, off} {
		got, err := c.HasMany(ctx, keys)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v", got)
		}
	}
	s := db.Set("goroute")
	_ = s.Add(ctx, keys[0])
	if got := mbs(s.HasMany(ctx, keys[:3])); fmt.Sprint(got) != "[true false false]" {
		t.Fatalf("named set: %v", got)
	}
}
