package cloner

import (
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

var (
	gLog *zerolog.Logger
	gCli *cli.Context
)

const (
	PrgmActionSync = uint8(iota)
	PrgmActionPrintGroups
	PrgmActionPrintRepositories
)

type Cloner struct{}

func NewCloner(l *zerolog.Logger) *Cloner {
	gLog = l
	return &Cloner{}
}

func (m *Cloner) PrintGroups(ctx *cli.Context) error {
	return m.Bootstrap(ctx, PrgmActionPrintGroups)
}

func (m *Cloner) PrintRepositories(ctx *cli.Context) error {
	return m.Bootstrap(ctx, PrgmActionPrintRepositories)
}

func (m *Cloner) Sync(ctx *cli.Context) error {
	return m.Bootstrap(ctx, PrgmActionSync)
}

func (m *Cloner) Bootstrap(ctx *cli.Context, action uint8) (e error) {
	gCli = ctx

	switch action {
	case PrgmActionPrintGroups:
		gLog.Debug().Msg("1111")
		gLog.Debug().Msg(gCli.Args().Get(0))
		var gl *glClient
		gl, e = newGlClient().connect(gCli.Args().Get(0))
		if e != nil {
			return
		}
		if e = gl.printGroupsAction(); e != nil {
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

func (m *Cloner) destruct() error { return nil }
