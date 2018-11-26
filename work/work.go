package work

import (
	"sync"
	"errors"
)

//type Worker interface {
//	Task()
//}

type Pool struct {
	work chan func()
	wg sync.WaitGroup
}

var ErrGoroutinesNum = errors.New("goroutines num too small")

func New(maxGoroutines int) (*Pool, error) {
	if maxGoroutines < 1 {
		return nil, ErrGoroutinesNum
	}
	p := Pool{
		work: make(chan func()),
	}

	p.wg.Add(maxGoroutines)
	for i:= 0; i < maxGoroutines; i++ {
		go func() {
			defer p.wg.Done()
			for w := range p.work {
				w()
			}
		}()
	}

	return &p, nil
}

func (p *Pool) Run(w func()) {
	p.work <- w
}

func (p *Pool) Shutdown() {
	close(p.work)
	p.wg.Wait()
}