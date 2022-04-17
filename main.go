package main

import (
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/MindHunter86/GitlabRepoCloner/cloner"
	"github.com/pkg/profile"

	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
)

var version = "devel" // -ldflags="-X 'main.version=X.X.X'"

func main() {
	defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()

	app := cli.NewApp()
	cli.VersionFlag = &cli.BoolFlag{Name: "print-version", Aliases: []string{"V"}}

	app.Name = "GitlabRepoCloner"
	app.Version = version
	app.Compiled = time.Now()
	app.Authors = []*cli.Author{
		&cli.Author{
			Name:  "Vadimka K.",
			Email: "admin@vkom.cc",
		},
	}
	app.Copyright = "(c) 2022 mindhunter86"
	app.Usage = "Gitlab clone tool for u're migrations"

	// application global flags
	app.Flags = []cli.Flag{
		// Some common options
		&cli.IntFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Value:   5,
			Usage:   "Verbose `LEVEL` (value from 5(debug) to 0(panic) and -1 for log disabling(quite mode))",
		},
		&cli.BoolFlag{
			Name:  "debug",
			Usage: "",
		},
		&cli.BoolFlag{
			Name:    "quite",
			Aliases: []string{"q"},
			Usage:   "Flag is equivalent to verbose -1",
		},
		&cli.DurationFlag{
			Name:  "http-client-timeout",
			Usage: "Internal HTTP client connection `TIMEOUT` (format: 1000ms, 1s)",
			Value: 10 * time.Second,
		},
		&cli.StringFlag{
			Name:  "http-client-user-agent",
			Usage: "Custom User-Agent for all requests",
		},
		&cli.BoolFlag{
			Name:  "http-client-insecure",
			Usage: "Flag for TLS certificate verification disabling",
		},
		&cli.BoolFlag{
			Name:  "http-client-insecure-ciphers",
			Usage: "Flag for avoiding of setting TLS min version to 1.2 and using secure ciphers",
		},

		// Queue settings
		&cli.IntFlag{
			Name:  "queue-workers",
			Value: 4,
			Usage: "queue-wrokers",
		},
		&cli.IntFlag{
			Name:  "queue-job-buffer",
			Value: 128,
			Usage: "queue-job-buffer",
		},

		// System settings

		// Application options
		// - build group tree with name or path
	}

	log := zerolog.New(zerolog.ConsoleWriter{
		Out: os.Stderr,
	}).With().Timestamp().Logger().Hook(SeverityHook{})
	zerolog.TimeFieldFormat = time.RFC3339Nano
	log.Debug().Msg("starting...")

	app.Commands = []*cli.Command{
		&cli.Command{
			Name:    "list",
			Aliases: []string{"l"},
			Usage:   "list gitlab components",
			Subcommands: []*cli.Command{
				&cli.Command{
					Name:  "groups",
					Usage: "list gitlab groups",
					Action: func(c *cli.Context) error {
						// TODO
						// if c.Int("verbose") < -1 || c.Int("verbose") > 5 {
						// 	log.Fatal().Msg("There is invalid data in verbose option. Option supports values for -1 to 5")
						// }

						// zerolog.SetGlobalLevel(zerolog.Level(int8((c.Int("verbose") - 5) * -1)))
						// if c.Int("verbose") == -1 || c.Bool("quite") {
						// 	zerolog.SetGlobalLevel(zerolog.Disabled)
						// }

						zerolog.SetGlobalLevel(zerolog.DebugLevel)
						return cloner.NewCloner(&log, c).PrintGroups()
					},
				},
				&cli.Command{
					Name:  "repositories",
					Usage: "list gitlab repositories",
					Action: func(c *cli.Context) error {
						zerolog.SetGlobalLevel(zerolog.DebugLevel)
						return cloner.NewCloner(&log, c).PrintRepositories()
					},
				},
			},
		},
		&cli.Command{
			Name:    "sync",
			Aliases: []string{"l"},
			Usage:   "list gitlab components",
			Action: func(c *cli.Context) error {
				return nil
			},
		},
	}

	// app.Action = func(c *cli.Context) (e error) {

	// if c.Int("verbose") < -1 || c.Int("verbose") > 5 {
	// 	log.Fatal().Msg("There is invalid data in verbose option. Option supports values for -1 to 5")
	// }

	// zerolog.SetGlobalLevel(zerolog.Level(int8((c.Int("verbose") - 5) * -1)))
	// if c.Int("verbose") == -1 || c.Bool("quite") {
	// 	zerolog.SetGlobalLevel(zerolog.Disabled)
	// }

	// return cloner.NewCloner(&log).Bootstrap(c) // Application starts here:
	// 	return
	// }

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	if e := app.Run(os.Args); e != nil {
		log.Fatal().Err(e).Msg("")
	}
}

type SeverityHook struct{}

func (h SeverityHook) Run(e *zerolog.Event, level zerolog.Level, msg string) {
	if level != zerolog.DebugLevel {
		return
	}

	rfn := "unknown"
	pcs := make([]uintptr, 1)

	if runtime.Callers(4, pcs) != 0 {
		if fun := runtime.FuncForPC(pcs[0] - 1); fun != nil {
			rfn = fun.Name()
		}
	}

	fn := strings.Split(rfn, "/")
	e.Str("func", fn[len(fn)-1:][0])
}
