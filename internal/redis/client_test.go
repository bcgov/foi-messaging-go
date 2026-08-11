package redis

import "testing"

func TestNewClient_SetsOptions(t *testing.T) {
	client := NewClient(ClientOptions{
		Address:  "localhost:6380",
		Username: "svc",
		Password: "secret",
		DB:       2,
		PoolSize: 15,
	})
	defer client.Close()

	opts := client.Options()
	if opts.Addr != "localhost:6380" {
		t.Errorf("Addr = %q, want %q", opts.Addr, "localhost:6380")
	}
	if opts.Username != "svc" {
		t.Errorf("Username = %q, want %q", opts.Username, "svc")
	}
	if opts.Password != "secret" {
		t.Errorf("Password = %q, want %q", opts.Password, "secret")
	}
	if opts.DB != 2 {
		t.Errorf("DB = %d, want 2", opts.DB)
	}
	if opts.PoolSize != 15 {
		t.Errorf("PoolSize = %d, want 15", opts.PoolSize)
	}
}
