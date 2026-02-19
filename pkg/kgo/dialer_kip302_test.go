package kgo

import (
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeResolver struct {
	lookup func(context.Context, string) ([]net.IPAddr, error)
}

func (f fakeResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	return f.lookup(ctx, host)
}

func TestDefaultDialerRotatesResolvedIPs(t *testing.T) {
	d := newDefaultDialer(time.Second, nil)
	lookups := 0
	d.resolver = fakeResolver{
		lookup: func(context.Context, string) ([]net.IPAddr, error) {
			lookups++
			return []net.IPAddr{
				{IP: net.ParseIP("10.0.0.1")},
				{IP: net.ParseIP("10.0.0.2")},
			}, nil
		},
	}

	var attempts []string
	d.plainDial = func(_ context.Context, _, addr string) (net.Conn, error) {
		attempts = append(attempts, addr)
		c1, c2 := net.Pipe()
		c2.Close()
		return c1, nil
	}

	conn, err := d.DialContext(context.Background(), "tcp", "broker.example:9092")
	if err != nil {
		t.Fatalf("unexpected dial error: %v", err)
	}
	conn.Close()

	conn, err = d.DialContext(context.Background(), "tcp", "broker.example:9092")
	if err != nil {
		t.Fatalf("unexpected second dial error: %v", err)
	}
	conn.Close()

	conn, err = d.DialContext(context.Background(), "tcp", "broker.example:9092")
	if err != nil { // list exhausted, should re-resolve and still succeed
		t.Fatalf("unexpected third dial error: %v", err)
	}
	conn.Close()

	got := strings.Join(attempts, ",")
	exp := "10.0.0.1:9092,10.0.0.2:9092,10.0.0.1:9092"
	if got != exp {
		t.Fatalf("unexpected attempt order, got %q != exp %q", got, exp)
	}
	if lookups != 2 {
		t.Fatalf("unexpected lookup count, got %d != exp 2", lookups)
	}
}

func TestDefaultDialerTriesAllResolvedIPs(t *testing.T) {
	d := newDefaultDialer(time.Second, nil)
	d.resolver = fakeResolver{
		lookup: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{
				{IP: net.ParseIP("10.0.0.1")},
				{IP: net.ParseIP("10.0.0.2")},
				{IP: net.ParseIP("10.0.0.3")},
			}, nil
		},
	}

	var attempts []string
	d.plainDial = func(_ context.Context, _, addr string) (net.Conn, error) {
		attempts = append(attempts, addr)
		return nil, io.EOF
	}

	_, err := d.DialContext(context.Background(), "tcp", "broker.example:9092")
	if err == nil {
		t.Fatal("expected dial error, got nil")
	}
	if !strings.Contains(err.Error(), "unable to dial any resolved IP") {
		t.Fatalf("expected aggregate dial error, got %v", err)
	}

	got := strings.Join(attempts, ",")
	exp := "10.0.0.1:9092,10.0.0.2:9092,10.0.0.3:9092"
	if got != exp {
		t.Fatalf("unexpected attempts, got %q != exp %q", got, exp)
	}
}

func TestDefaultDialerLiteralIPBypassesResolver(t *testing.T) {
	d := newDefaultDialer(time.Second, nil)
	d.resolver = fakeResolver{
		lookup: func(context.Context, string) ([]net.IPAddr, error) {
			t.Fatal("resolver should not be called for IP literals")
			return nil, nil
		},
	}

	var attempts []string
	d.plainDial = func(_ context.Context, _, addr string) (net.Conn, error) {
		attempts = append(attempts, addr)
		c1, c2 := net.Pipe()
		c2.Close()
		return c1, nil
	}

	conn, err := d.DialContext(context.Background(), "tcp", "192.0.2.10:9092")
	if err != nil {
		t.Fatalf("unexpected dial error: %v", err)
	}
	conn.Close()

	if len(attempts) != 1 || attempts[0] != "192.0.2.10:9092" {
		t.Fatalf("unexpected attempts: %v", attempts)
	}
}

func TestDefaultDialerConcurrentReservations(t *testing.T) {
	d := newDefaultDialer(time.Second, nil)

	var (
		mu      sync.Mutex
		lookups int
	)
	d.resolver = fakeResolver{
		lookup: func(context.Context, string) ([]net.IPAddr, error) {
			mu.Lock()
			lookups++
			mu.Unlock()

			ips := make([]net.IPAddr, 0, 64)
			for i := 0; i < 64; i++ {
				ips = append(ips, net.IPAddr{IP: net.ParseIP("10.0.0." + strconv.Itoa(i+1))})
			}
			return ips, nil
		},
	}

	var attempted sync.Map
	d.plainDial = func(_ context.Context, _, addr string) (net.Conn, error) {
		if _, loaded := attempted.LoadOrStore(addr, struct{}{}); loaded {
			t.Errorf("duplicate first-attempt address under concurrency: %s", addr)
		}
		c1, c2 := net.Pipe()
		c2.Close()
		return c1, nil
	}

	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			conn, err := d.DialContext(context.Background(), "tcp", "broker.example:9092")
			if err != nil {
				t.Errorf("unexpected dial error: %v", err)
				return
			}
			conn.Close()
		}()
	}
	wg.Wait()

	gotAttempts := 0
	attempted.Range(func(_, _ any) bool {
		gotAttempts++
		return true
	})
	if gotAttempts != n {
		t.Fatalf("unexpected number of unique attempts, got %d != exp %d", gotAttempts, n)
	}

	mu.Lock()
	defer mu.Unlock()
	if lookups != 1 {
		t.Fatalf("unexpected lookup count under concurrency, got %d != exp 1", lookups)
	}
}

func TestDefaultDialerWrapsFromNextIndex(t *testing.T) {
	d := newDefaultDialer(time.Second, nil)
	d.resolver = fakeResolver{
		lookup: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{
				{IP: net.ParseIP("10.0.0.1")},
				{IP: net.ParseIP("10.0.0.2")},
			}, nil
		},
	}

	var attempts []string
	secondCall := false
	d.plainDial = func(_ context.Context, _, addr string) (net.Conn, error) {
		attempts = append(attempts, addr)
		if secondCall && strings.HasPrefix(addr, "10.0.0.2:") {
			return nil, io.EOF
		}
		c1, c2 := net.Pipe()
		c2.Close()
		return c1, nil
	}

	conn, err := d.DialContext(context.Background(), "tcp", "broker.example:9092")
	if err != nil {
		t.Fatalf("unexpected first dial error: %v", err)
	}
	conn.Close()

	secondCall = true
	conn, err = d.DialContext(context.Background(), "tcp", "broker.example:9092")
	if err != nil {
		t.Fatalf("unexpected second dial error: %v", err)
	}
	conn.Close()

	got := strings.Join(attempts, ",")
	exp := "10.0.0.1:9092,10.0.0.2:9092,10.0.0.1:9092"
	if got != exp {
		t.Fatalf("unexpected wrap attempts, got %q != exp %q", got, exp)
	}
}

func TestDefaultDialerUsesSingleAddressFamily(t *testing.T) {
	d := newDefaultDialer(time.Second, nil)
	d.resolver = fakeResolver{
		lookup: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{
				{IP: net.ParseIP("10.0.0.1")},
				{IP: net.ParseIP("2001:db8::1")},
				{IP: net.ParseIP("10.0.0.2")},
			}, nil
		},
	}

	var attempts []string
	d.plainDial = func(_ context.Context, _, addr string) (net.Conn, error) {
		attempts = append(attempts, addr)
		return nil, io.EOF
	}

	_, err := d.DialContext(context.Background(), "tcp", "broker.example:9092")
	if err == nil {
		t.Fatal("expected dial error")
	}

	got := strings.Join(attempts, ",")
	exp := "10.0.0.1:9092,10.0.0.2:9092"
	if got != exp {
		t.Fatalf("unexpected attempts by family, got %q != exp %q", got, exp)
	}
}
