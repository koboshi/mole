package runner

import (
	"os"
	"time"
	"errors"
	"os/signal"
)

type Runner struct {

	//能够接收和发送操作系统信号的通道
	interrupt chan os.Signal

	//能够接收和发送error的通道
	complete chan error

	//只能接收(读)time.Time的通道
	timeout <-chan time.Time

	tasks []func(int)
}

var ErrTimeout = errors.New("received timeout")

var ErrInterrupt = errors.New("received interrupt")

func New(d time.Duration) *Runner {
	runner := new(Runner)
	runner.interrupt = make(chan  os.Signal, 1)//容量为1，接收和发送不会堵塞
	runner.complete = make(chan error)
	runner.timeout = time.After(d)

	return runner
}

func (r *Runner) Add(tasks ...func(int)) {
	r.tasks = append(r.tasks, tasks...)
}

func (r *Runner) Start() error {
	signal.Notify(r.interrupt, os.Interrupt)

	go func() {
		r.complete <- r.run()
	}()

	select {
	case err := <-r.complete:
		return err
	case <-r.timeout:
		return ErrTimeout
	}
}

func (r *Runner) run() error {
	for id, task := range r.tasks {
		if r.gotInterrupt() {
			return ErrInterrupt
		}
		task(id)
	}
	return nil
}

func (r *Runner) gotInterrupt() bool {
	select {
	case <-r.interrupt:
		signal.Stop(r.interrupt)
		return true
	default:
		return false
	}
}