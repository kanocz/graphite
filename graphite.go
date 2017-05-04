package graphite

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sync/atomic"
	"time"
)

// Graphite represents metrics to graphite sender
type Graphite struct {
	addr      *net.TCPAddr
	sendEvery time.Duration
	prefix    string
	msgCount  int32       // atomic counter
	messages  chan string // we doesn't need anything more then string
}

// New inicialized Graphite instance
func New(addr string, sendEvery time.Duration, prefix string, buflen int) (*Graphite, error) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", addr)
	if nil != err {
		return nil, err
	}

	g := Graphite{
		addr:      tcpaddr,
		sendEvery: sendEvery,
		prefix:    prefix,
		messages:  make(chan string, buflen),
	}

	go g.run()

	return &g, nil
}

func (g *Graphite) pushU64(metric string, value uint64) {
	msg := fmt.Sprintf("%s.%s %d %d", g.prefix, metric, value, time.Now().Unix())
	select {
	case g.messages <- msg:
		atomic.AddInt32(&g.msgCount, 1)
	default:
		log.Println("graphite buffer full!")
	}
}

func (g *Graphite) pushF64(metric string, value float64) {
	msg := fmt.Sprintf("%s.%s %.4f %d", g.prefix, metric, value, time.Now().Unix())
	select {
	case g.messages <- msg:
		atomic.AddInt32(&g.msgCount, 1)
	default:
		log.Println("graphite buffer full!")
	}
}

func (g *Graphite) run() {
	for range time.Tick(g.sendEvery) {
		if atomic.LoadInt32(&g.msgCount) == 0 {
			continue
		}

		// now := strconv.FormatInt(time.Now().Unix(), 10)

		err := func() error {
			conn, err := net.DialTCP("tcp", nil, g.addr)
			if nil != err {
				return err
			}
			defer conn.Close()

			out := bufio.NewWriter(conn)

			for {
				select {
				case msg := <-g.messages:
					atomic.AddInt32(&g.msgCount, -1)
					_, err := fmt.Fprintf(out, msg)
					if nil != err {
						return err
					}
					err = out.Flush()
					if nil != err {
						return err
					}
				default:
					return nil
				}
			}
		}()

		if nil != err {
			log.Println("graphite push error: ", err)
		}
	}
}
