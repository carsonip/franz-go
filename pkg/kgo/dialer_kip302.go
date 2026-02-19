package kgo

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

type ipResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

// defaultDialer implements KIP-302 / KIP-602 behavior:
// for hostnames, resolve all IPs and rotate dial attempts across them.
type defaultDialer struct {
	dialer *net.Dialer
	tls    *tls.Config

	plainDial func(context.Context, string, string) (net.Conn, error)
	resolver  ipResolver

	mu     sync.Mutex
	states map[string]*hostDialState
}

type hostDialState struct {
	mu sync.Mutex

	ips  []string
	next int // number of addresses used in this resolution cycle

	resolving bool
	waiters   chan struct{}
}

func newDefaultDialer(timeout time.Duration, tlsCfg *tls.Config) *defaultDialer {
	dialer := &net.Dialer{Timeout: timeout}
	return &defaultDialer{
		dialer:    dialer,
		tls:       tlsCfg,
		plainDial: dialer.DialContext,
		resolver:  net.DefaultResolver,
		states:    make(map[string]*hostDialState),
	}
}

func (d *defaultDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid dial address %q: %w", addr, err)
	}
	if ip := net.ParseIP(host); ip != nil {
		return d.dialOne(ctx, network, addr, host)
	}

	ips, start, err := d.reserveStart(ctx, host)
	if err != nil {
		return nil, err
	}

	var dialErrs []error
	for i := 0; i < len(ips); i++ {
		idx := (start + i) % len(ips)
		ip := ips[idx]
		target := net.JoinHostPort(ip, port)
		conn, err := d.dialOne(ctx, network, target, host)
		if err == nil {
			d.commitProgress(host, ips, start+i+1)
			return conn, nil
		}
		dialErrs = append(dialErrs, fmt.Errorf("%s: %w", target, err))
	}
	d.commitProgress(host, ips, start+len(ips))
	return nil, fmt.Errorf("unable to dial any resolved IP for %s: %w", addr, errors.Join(dialErrs...))
}

func (d *defaultDialer) dialOne(ctx context.Context, network, targetAddr, serverName string) (net.Conn, error) {
	if d.tls == nil {
		return d.plainDial(ctx, network, targetAddr)
	}
	c := d.tls.Clone()
	if c.ServerName == "" {
		c.ServerName = serverName
	}
	return (&tls.Dialer{
		NetDialer: d.dialer,
		Config:    c,
	}).DialContext(ctx, network, targetAddr)
}

func (d *defaultDialer) lookupHost(ctx context.Context, host string) ([]string, error) {
	ips, err := d.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	deduped := make([]string, 0, len(ips))
	seen := make(map[string]struct{}, len(ips))
	firstFamily := 0 // 4 or 6 once set
	for _, ip := range ips {
		parsed := ip.IP
		if parsed == nil {
			continue
		}
		family := 6
		if parsed.To4() != nil {
			family = 4
		}
		if firstFamily == 0 {
			firstFamily = family
		}
		if family != firstFamily {
			continue
		}
		s := ip.IP.String()
		if s == "<nil>" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		deduped = append(deduped, s)
	}
	return deduped, nil
}

func (d *defaultDialer) reserveStart(ctx context.Context, host string) ([]string, int, error) {
	state := d.hostState(host)
	for {
		state.mu.Lock()
		if len(state.ips) > 0 && state.next < len(state.ips) {
			start := state.next
			state.next++
			ips := append([]string(nil), state.ips...)
			state.mu.Unlock()
			return ips, start, nil
		}

		if state.resolving {
			wait := state.waiters
			state.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-wait:
			}
			continue
		}
		state.resolving = true
		state.waiters = make(chan struct{})
		state.mu.Unlock()

		resolved, err := d.lookupHost(ctx, host)

		state.mu.Lock()
		if len(resolved) > 0 && err == nil {
			state.ips = resolved
			state.next = 0
		}
		state.resolving = false
		wait := state.waiters
		state.waiters = nil
		state.mu.Unlock()

		close(wait)

		if err != nil {
			return nil, 0, fmt.Errorf("unable to resolve host %q: %w", host, err)
		}
		if len(resolved) == 0 {
			return nil, 0, fmt.Errorf("host %q resolved to no IPs", host)
		}
	}
}

func (d *defaultDialer) commitProgress(host string, ips []string, next int) {
	state := d.hostState(host)
	state.mu.Lock()
	if sameIPs(state.ips, ips) && state.next < next {
		state.next = next
	}
	state.mu.Unlock()
}

func (d *defaultDialer) hostState(host string) *hostDialState {
	d.mu.Lock()
	state := d.states[host]
	if state == nil {
		state = &hostDialState{}
		d.states[host] = state
	}
	d.mu.Unlock()
	return state
}

func sameIPs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
