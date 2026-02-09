package relay

import (
	"io"
	"net"
	"sync"
)

// Relay performs bidirectional data copy between two connections.
// It blocks until both directions are closed or an error occurs.
func Relay(conn1, conn2 net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	// conn1 -> conn2
	go func() {
		defer wg.Done()
		io.Copy(conn2, conn1)
		// Signal EOF to the other side
		if c, ok := conn2.(interface{ CloseWrite() error }); ok {
			c.CloseWrite()
		}
	}()

	// conn2 -> conn1
	go func() {
		defer wg.Done()
		io.Copy(conn1, conn2)
		// Signal EOF to the other side
		if c, ok := conn1.(interface{ CloseWrite() error }); ok {
			c.CloseWrite()
		}
	}()

	wg.Wait()

	conn1.Close()
	conn2.Close()
}
