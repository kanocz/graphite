package graphite

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
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

// PushU64 sends Uint64 value to graphite
func (g *Graphite) PushU64(metric string, value uint64) {
	g.Push(metric, strconv.FormatUint(value, 10))
}

// PushI64 sends Int64 value to graphite
func (g *Graphite) PushI64(metric string, value int64) {
	g.Push(metric, strconv.FormatInt(value, 10))
}

// PushF64 sends Float64 value to graphite
func (g *Graphite) PushF64(metric string, value float64) {
	g.Push(metric, strconv.FormatFloat(value, 'f', 4, 64))
}

// Push sends pre-formated (string) value to graphite
func (g *Graphite) Push(metric string, value string) {
	msg := fmt.Sprintf("%s.%s %s %d\n", g.prefix, metric, value, time.Now().Unix())
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

		err := func() error {
			conn, err := net.DialTCP("tcp", nil, g.addr)
			if nil != err {
				return err
			}
			defer func() {
				err := conn.Close()
				if nil != err {
					log.Println("Error closing TCP connection to graphite: ", err)
				}
			}()

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
