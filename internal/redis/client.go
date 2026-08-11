package redis

import (
	"crypto/tls"

	goredis "github.com/redis/go-redis/v9"
)

// ClientOptions configures the go-redis client this package constructs.
type ClientOptions struct {
	Address  string
	Username string
	Password string
	TLS      *tls.Config
	DB       int
	PoolSize int
}

// NewClient builds a go-redis client. Construction does not dial Redis —
// connections are established lazily on first use.
func NewClient(opts ClientOptions) *goredis.Client {
	return goredis.NewClient(&goredis.Options{
		Addr:      opts.Address,
		Username:  opts.Username,
		Password:  opts.Password,
		TLSConfig: opts.TLS,
		DB:        opts.DB,
		PoolSize:  opts.PoolSize,
	})
}
