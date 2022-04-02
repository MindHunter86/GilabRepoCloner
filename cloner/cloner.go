package cloner

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

var (
	gLog *zerolog.Logger
	gCli *cli.Context

	gAbort context.CancelFunc
)

const (
	PrgmActionSync = uint8(iota)
	PrgmActionPrintGroups
	PrgmActionPrintRepositories
)

type Cloner struct{}

func NewCloner(l *zerolog.Logger, c *cli.Context) *Cloner {
	gLog, gCli = l, c
	return &Cloner{}
}

func (m *Cloner) PrintGroups() error {
	return m.Bootstrap(PrgmActionPrintGroups)
}

func (m *Cloner) PrintRepositories() error {
	return m.Bootstrap(PrgmActionPrintRepositories)
}

func (m *Cloner) Sync() error {
	return m.Bootstrap(PrgmActionSync)
}

func (m *Cloner) Bootstrap(action uint8) (e error) {
	kernSignal := make(chan os.Signal, 1)
	signal.Notify(kernSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGTERM, syscall.SIGQUIT)

	var cancelCtx context.Context
	cancelCtx, gAbort = context.WithCancel(context.WithValue(context.Background(), contextKeyKernSignal, kernSignal))

	wg, ep := sync.WaitGroup{}, make(chan error, 1)
	go m.loop(cancelCtx, ep, wg.Done)

	switch action {
	case PrgmActionPrintGroups:
		var gl *glClient
		gl, e = newGlClient().connect(gCli.Args().Get(0))
		if e != nil {
			return
		}
		if e = gl.printGroupsAction(); e != nil {
			return
		}
	case PrgmActionPrintRepositories:
		var gl *glClient
		gl, e = newGlClient().connect(gCli.Args().Get(0))
		if e != nil {
			return
		}
		if e = gl.printRepositoriesAction(); e != nil {
			return
		}
	default:
		break
	}

	if err := m.destruct(); err != nil {
		gLog.Warn().Err(err).Msg("Abnormal destruct status!")
	}

	return e
}

func (m *Cloner) loop(ctx context.Context, errors chan error, done func()) {
	// var err error
	kernSignal := ctx.Value("kernSignal").(chan os.Signal)

LOOP:
	for {
		select {
		case <-kernSignal:
			gLog.Info().Msg("Syscall.SIG* has been detected! Closing application...")
			gAbort()
			break LOOP
		// case err = <-errors:
		// 	if err != nil {
		// 		gLog.Error().Err(err).Msg("Fatal Runtime Error!!! Abnormal application closing ...")
		// 		break LOOP
		// 	}
		case <-ctx.Done():
			break LOOP
		}
	}
}

func (m *Cloner) destruct() error { return nil }
