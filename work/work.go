package work

import "sync"

//type Worker interface {
//	Task()
//}

type Pool struct {
	work chan func()
	wg sync.WaitGroup
}

func New(MaxGoroutines int) *Pool {
	p := Pool{
		work: make(chan func()),
	}

	p.wg.Add(MaxGoroutines)
	for i:= 0; i < MaxGoroutines; i++ {
		go func() {
			defer p.wg.Done()
			for w := range p.work {
				w()
			}
		}()
	}

	return &p
}

func (p *Pool) Run(w func()) {
	p.work <- w
}

func (p *Pool) Shutdown() {
	close(p.work)
	p.wg.Wait()
}