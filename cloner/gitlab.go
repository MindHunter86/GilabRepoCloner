package cloner

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/xanzy/go-gitlab"
)

type glClient struct {
	instance *gitlab.Client

	endpoint    *url.URL
	groupPrefix string
	apiToken    string
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
	return gitlab.NewClient(m.apiToken,
		gitlab.WithBaseURL(m.endpoint.String()),
		gitlab.WithHTTPClient(&http.Client{
			Timeout: gCli.Duration("http-client-timeout"),
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: gCli.Bool("http-client-insecure"),
				},
				DisableCompression: false,
			},
		}))
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
	// var mu sync.Mutex

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
					// mu.Lock()
					projects = append(projects, prjs...)
					// mu.Unlock()

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

			// TODO ERROR HANDLING
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
					// TODO ADD ERROR MESSAGE HANDLING
					return nil, e
				}

				return prjs, e
			}, args, jobsWait.Done)
			jb.assignCollector(collector.jobsChannel)

			jobsWait.Add(1)
			gQueue <- jb
		}

		gLog.Debug().Msg("groups scaning was finished")

		// TODO maybe it's a bad idia
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
	var rsp *gitlab.Response = &gitlab.Response{}

	var jobsWait sync.WaitGroup
	var collectorWait sync.WaitGroup
	grpsChannel := make(chan *[]*gitlab.Group, gCli.Int("queue-workers"))

	// job responses collector:
	collectorWait.Add(1)
	go func(done func()) {
		defer done()

		gLog.Debug().Msgf("there is %d jobs in queue", len(grpsChannel))
		defer gLog.Debug().Msg("jobs result collector has been finished")

		var ok bool
		var jobGroups *[]*gitlab.Group

		for {
			jobGroups, ok = <-grpsChannel
			gLog.Debug().Msg("there is new result from job, handling...")
			if !ok || gCtx.Err() != nil {
				gLog.Debug().Msgf("result pipeline has been closed. Len %d", len(grpsChannel))
				return
			}

			groups = append(groups, *jobGroups...)
		}
	}(collectorWait.Done)

	// job spawner:
	for nextPage := 0; nextPage <= rsp.TotalPages && gCtx.Err() == nil; nextPage++ {

		// first call for totalPages variable get
		if rsp.TotalPages == 0 {
			if grp, rsp, e = m.getGroupsFromPage(rsp.NextPage); e == nil {
				grp = m.getMatchedGroups(grp)
				grpsChannel <- &grp

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

		// TODO ERROR HANDLING
		// async calls (jobs spawn)
		args := map[string]interface{}{
			"page":    nextPage,
			"channel": grpsChannel,
			"add":     jobsWait.Add,
			"done":    jobsWait.Done,
		}

		jobsWait.Add(1)
		gQueue <- &job{
			fn: func(payload map[string]interface{}) (interface{}, error) {
				defer payload["done"].(func())()
				// payload["add"].(func(int))(1)

				page := payload["page"].(int)
				pChannel := payload["channel"].(chan *[]*gitlab.Group)

				gLog.Debug().Msgf("There is new job with page %d", page)

				grps, _, e := m.getGroupsFromPage(page)
				if e != nil {
					gLog.Error().Err(e).Msg("There is abnormal result from Gitlab API")
					// TODO ADD ERROR MESSAGE HANDLING
					return nil, e
				}

				grps = m.getMatchedGroups(grps)

				gLog.Debug().Msg("trying to push projects in pipe")
				select {
				case <-gCtx.Done():
					return nil, nil
				case pChannel <- &grps:
					gLog.Debug().Msgf("successfully pushed projects for page %d", page)
				}

				gLog.Debug().Msg("all done, job can be stopped now")
				return nil, nil
			},
			args: args,
		}

		gLog.Debug().Msg("groups scaning was finished")
	}

	gLog.Debug().Msg("all jobs were spawned, waiting...")
	jobsWait.Wait()

	gLog.Debug().Msg("all jobs are executed, close result pipeline")
	close(grpsChannel)

	collectorWait.Wait()
	return
}

func (m *glClient) getGroupsFromPage(page int) ([]*gitlab.Group, *gitlab.Response, error) {
	gLog.Debug().Msgf("Called with page %d ", page)

	listOptions := gitlab.ListOptions{}
	if page != 0 {
		listOptions.Page = page
	}

	return m.instance.Groups.ListGroups(&gitlab.ListGroupsOptions{
		ListOptions:  listOptions,
		AllAvailable: gitlab.Bool(true),
		TopLevelOnly: gitlab.Bool(true),
	})
}

func (m *glClient) getMatchedGroups(groups []*gitlab.Group) (matchedGroups []*gitlab.Group) {
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
