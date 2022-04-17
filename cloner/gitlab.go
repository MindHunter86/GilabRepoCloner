package cloner

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/xanzy/go-gitlab"
)

type glClient struct {
	instance *gitlab.Client

	endpoint    *url.URL
	groupPrefix string
	apiToken    string

	inner http.RoundTripper
}

func newGlClient() *glClient {
	return &glClient{}
}

func (m *glClient) connect(arg string) (*glClient, error) {
	var e error

	if m.endpoint, e = url.Parse(arg); e != nil {
		return m, e
	}

	buf := strings.Split(m.endpoint.EscapedPath(), "/")
	if len(buf) > 0 {
		m.groupPrefix = buf[len(buf)-1]
		m.endpoint.Path = "/" + strings.Join(buf[:len(buf)-1], "/")
		gLog.Debug().Msg(m.endpoint.RawPath)
		gLog.Debug().Msg(m.endpoint.String())
		gLog.Debug().Msg("found group prefix " + m.groupPrefix)
	}

	m.apiToken = m.endpoint.User.Username()
	m.endpoint.User = nil

	m.instance, e = m.setGitlabConnection()
	return m, e
}

func (m *glClient) setGitlabConnection() (*gitlab.Client, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: gCli.Bool("http-client-insecure"),
	}

	if !gCli.Bool("http-client-insecure-ciphers") {
		tlsConfig.MinVersion = tls.VersionTLS12
		tlsConfig.CipherSuites = []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		}
	}
	return gitlab.NewClient(m.apiToken,
		gitlab.WithBaseURL(m.endpoint.String()),
		gitlab.WithHTTPClient(&http.Client{
			Timeout: gCli.Duration("http-client-timeout"),
			Transport: m.setGitlabUserAgent(&http.Transport{
				DisableKeepAlives:   false,
				IdleConnTimeout:     300 * time.Second,
				MaxIdleConnsPerHost: 128,
				TLSClientConfig:     tlsConfig,
				DisableCompression:  false,
			}),
		}))
}

func (m *glClient) setGitlabUserAgent(inner http.RoundTripper) http.RoundTripper {
	m.inner = inner
	return m
}

func (m *glClient) RoundTrip(r *http.Request) (*http.Response, error) {
	if len(gCli.String("http-client-user-agent")) != 0 {
		r.Header.Set("User-Agent", gCli.String("http-client-user-agent"))
	} else {
		r.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0")
	}
	return m.inner.RoundTrip(r)
}

func (m *glClient) printGroupsAction() (e error) {
	var groups []*gitlab.Group

	if groups, e = m.getInstanceGroupsAsync(); e != nil {
		return
	}

	m.printGroups(groups)
	return
}

func (m *glClient) printRepositoriesAction() (e error) {
	var groups []*gitlab.Group
	var projects []*gitlab.Project

	if groups, e = m.getInstanceGroupsAsync(); e != nil {
		return
	}

	if projects, e = m.getInstanceProjectsAsync(groups); e != nil {
		return
	}

	m.printProjects(projects)
	return
}

func (m *glClient) getInstanceProjectsAsync(groups []*gitlab.Group) (projects []*gitlab.Project, e error) {
	var prjs []*gitlab.Project
	var jobsWait sync.WaitGroup

	responsePool := sync.Pool{
		New: func() interface{} {
			return &gitlab.Response{}
		},
	}

	// job responses collector:
	collector := newCollector()
	collector.wg.Add(2)
	go func() {
		defer collector.wg.Done()

		for _, payload := range collector.collect() {
			result := payload.(*jobResult)
			if result.err != nil {
				gLog.Error().Err(result.err).Msg("")
				continue
			}

			projects = append(projects, result.payload.([]*gitlab.Project)...)
		}
	}()

	// job spawner:
	for i, group := range groups {
		gLog.Debug().Msgf("there are %d groups waiting for scaning; scan #%d", len(groups), i)
		rsp := responsePool.Get().(*gitlab.Response)

		for nextPage := 0; nextPage <= rsp.TotalPages && gCtx.Err() == nil; nextPage++ {

			// first call for totalPages variable get
			if rsp.TotalPages == 0 {
				if prjs, rsp, e = m.getProjectsFromPage(group.ID, rsp.NextPage); e == nil {
					projects = append(projects, prjs...)

					if rsp.NextPage == 0 {
						break
					}

					nextPage = rsp.NextPage
					gLog.Debug().Msgf("nextpage %d", rsp.NextPage)
					gLog.Debug().Msgf("total pages %d", rsp.TotalPages)
				} else {
					gLog.Error().Err(e).Msg("There is abnraml result from Gitlab API")
					return
				}
			}

			// async calls (jobs spawn)
			args := map[string]interface{}{
				"page":  nextPage,
				"group": group.ID,
			}

			jb := newJob(func(payload map[string]interface{}) (interface{}, error) {
				defer gLog.Debug().Msg("all done, job can be stopped now")

				page, group := payload["page"].(int), payload["group"].(int)
				gLog.Debug().Msgf("There is new job with page %d and group %d", page, group)

				prjs, _, e := m.getProjectsFromPage(group, page)
				if e != nil {
					gLog.Error().Err(e).Msg("There is abnormal result from Gitlab API")
					return nil, e
				}

				return prjs, e
			}, args, jobsWait.Done)
			jb.assignCollector(collector.jobsChannel)

			jobsWait.Add(1)
			gQueue <- jb
		}

		gLog.Debug().Msg("groups scaning was finished")

		// ??
		// in my minds it looks as getting Pooled struct
		// and then destroy it via setting nil to pointer.
		// So GC must clear it. Maybe it's a very bad idea.
		rsp = nil
	}

	gLog.Debug().Msg("all jobs were spawned, waiting...")
	jobsWait.Wait()

	gLog.Debug().Msg("all jobs are executed, close collector pipeline")
	close(collector.jobsChannel)
	collector.wg.Wait()
	return
}

func (m *glClient) getProjectsFromPage(gid, page int) ([]*gitlab.Project, *gitlab.Response, error) {
	gLog.Debug().Msgf("Called with gid %d, page %d", gid, page)

	listOptions := gitlab.ListOptions{}
	if page != 0 {
		listOptions.Page = page
	}

	return m.instance.Groups.ListGroupProjects(gid, &gitlab.ListGroupProjectsOptions{
		ListOptions:      listOptions,
		IncludeSubgroups: gitlab.Bool(true),
	})
}

func (m *glClient) getInstanceGroupsAsync() (groups []*gitlab.Group, e error) {
	var grp []*gitlab.Group
	var jobsWait sync.WaitGroup
	var rsp *gitlab.Response = &gitlab.Response{}

	// job responses collector:
	collector := newCollector()
	collector.wg.Add(2)
	go func() {
		defer collector.wg.Done()

		for _, payload := range collector.collect() {
			result := payload.(*jobResult)
			if result.err != nil {
				gLog.Error().Err(e).Msg("")
				continue
			}

			groups = append(groups, result.payload.([]*gitlab.Group)...)
		}
	}()

	// job spawner:
	for nextPage := 0; nextPage <= rsp.TotalPages && gCtx.Err() == nil; nextPage++ {

		// first call for totalPages variable get
		if rsp.TotalPages == 0 {
			if grp, rsp, e = m.getGroupsFromPage(rsp.NextPage); e == nil {
				groups = append(groups, m.getMatchedGroups(grp)...)

				gLog.Debug().Msgf("nextpage %d", rsp.NextPage)
				gLog.Debug().Msgf("total pages %d", rsp.TotalPages)

				if rsp.NextPage == 0 {
					break
				}

				nextPage = rsp.NextPage
			} else {
				gLog.Error().Err(e).Msg("There is abnraml result from Gitlab API")
				return
			}
		}

		// async calls (jobs spawn)
		args := map[string]interface{}{
			"page": nextPage,
		}

		jb := newJob(func(payload map[string]interface{}) (interface{}, error) {
			defer gLog.Debug().Msg("all done, job can be stopped now")

			page := payload["page"].(int)
			gLog.Debug().Msgf("There is new job with page %d", page)

			grps, _, e := m.getGroupsFromPage(page)
			if e != nil {
				gLog.Error().Err(e).Msg("There is abnormal result from Gitlab API")
				return nil, e
			}

			return m.getMatchedGroups(grps), e
		}, args, jobsWait.Done)
		jb.assignCollector(collector.jobsChannel)

		jobsWait.Add(1)
		gQueue <- jb

		gLog.Debug().Msg("groups scaning was finished")
	}

	gLog.Debug().Msg("all jobs were spawned, waiting...")
	jobsWait.Wait()

	gLog.Debug().Msg("all jobs are executed, close collector pipeline")
	close(collector.jobsChannel)
	collector.wg.Wait()
	return
}

func (m *glClient) getGroupsFromPage(page int) ([]*gitlab.Group, *gitlab.Response, error) {
	gLog.Debug().Msgf("Called with page %d ", page)

	listOptions := gitlab.ListOptions{}
	if page != 0 {
		listOptions.Page = page
	}

	listGroupOptions := &gitlab.ListGroupsOptions{
		ListOptions:  listOptions,
		TopLevelOnly: gitlab.Bool(true),
	}

	// ??
	if len(m.groupPrefix) != 0 {
		// listGroupOptions.TopLevelOnly = gitlab.Bool(false)
		listGroupOptions.AllAvailable = gitlab.Bool(true)
	}

	return m.instance.Groups.ListGroups(listGroupOptions)
}

func (m *glClient) getMatchedGroups(groups []*gitlab.Group) (matchedGroups []*gitlab.Group) {
	if len(m.groupPrefix) == 0 {
		return groups
	}

	for i, group := range groups {
		if group.FullPath == m.groupPrefix {
			matchedGroups = append(matchedGroups, group)
			continue
		}

		groups[i] = nil
	}

	return
}

func (m *glClient) printGroups(groups []*gitlab.Group) {
	t := table.NewWriter()
	defer t.Render()

	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"ID", "Path", "Name", "Visibility", "Parent ID", "Created At"})

	for _, group := range groups {
		if group == nil {
			continue
		}

		t.AppendRow([]interface{}{group.ID, group.FullPath, group.FullName, group.Visibility, group.ParentID, group.CreatedAt})
	}
}

func (m *glClient) printProjects(projects []*gitlab.Project) {
	t := table.NewWriter()
	defer t.Render()

	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"ID", "Path", "Name", "Visibility", "Created At", "Last Activity"})

	for _, project := range projects {
		t.AppendRow([]interface{}{
			project.ID, project.PathWithNamespace, project.Name, project.Visibility, project.CreatedAt, project.LastActivityAt,
		})
	}
}
