package pool

import (
	"sync"
	"io"
	"errors"
	"log"
)

type Pool struct {
	m sync.Mutex

	resources chan io.Closer

	factory func()(io.Closer, error)

	closed bool
}

var ErrPoolClosed = errors.New("pool has been closed")
var ErrPoolSize = errors.New("size value too small")

func New(fn func()(io.Closer, error), size uint) (*Pool, error) {
	if size <= 0 {
		return nil, ErrPoolSize
	}

	return &Pool{
		factory: fn,
		resources: make(chan io.Closer, size),
	}, nil
}

func (p *Pool) Acquire() (io.Closer, error) {
	select {
	case r, ok := <-p.resources:
		log.Println("Acquire:", "Shared Resources")
		if !ok {
			return nil, ErrPoolClosed
		}
		return r, nil
	default:
		log.Println("Acquire", "New Resource")
		return p.factory()
	}
}

func (p *Pool) Release(r io.Closer) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.closed {
		r.Close()
		return
	}

	select {
	case p.resources<-r:
		log.Println("Release:", "In Queue")
	default:
		log.Println("Release:", "Closing")
		r.Close()
	}
}

func (p *Pool) Close() {
	p.m.Lock()
	defer p.m.Unlock()

	if p.closed {
		return
	}

	p.closed = true

	close(p.resources)

	var wg sync.WaitGroup
	wg.Add(len(p.resources))
	for r:= range p.resources {
		go func() {
			defer wg.Done()
			r.Close()
		}()
	}
	wg.Wait()
}