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
	// if groups, e = m.getInstanceGroups(); e != nil {
	// 	return
	// }

	if groups, e = m.getInstanceGroupsAsync(); e != nil {
		return
	}

	m.printGroups(groups)
	return
}

func (m *glClient) printRepositoriesAction() (e error) {
	var groups []*gitlab.Group
	var projects []*gitlab.Project

	// if groups, e = m.getInstanceGroups(); e != nil {
	// 	return
	// }

	if groups, e = m.getInstanceGroupsAsync(); e != nil {
		return
	}

	// if projects, e = m.getInstanceProjects(groups); e != nil {
	// 	return
	// }

	if projects, e = m.getInstanceProjectsAsync(groups); e != nil {
		return
	}

	m.printProjects(projects)
	return
}

func (m *glClient) getInstanceProjects(groups []*gitlab.Group) (projects []*gitlab.Project, e error) {
	var prjs []*gitlab.Project
	var rsp *gitlab.Response = &gitlab.Response{}

	for _, group := range groups {
		for {
			if prjs, rsp, e = m.getProjectsFromPage(group.ID, rsp.NextPage); e == nil {
				projects = append(projects, prjs...)

				if rsp.NextPage == 0 {
					break
				}
			} else {
				break
			}
		}
	}

	return
}

func (m *glClient) getInstanceProjectsAsync(groups []*gitlab.Group) (projects []*gitlab.Project, e error) {
	var prjs []*gitlab.Project

	responsePool := sync.Pool{
		New: func() interface{} {
			return &gitlab.Response{}
		},
	}

	var prjsChannel chan *[]*gitlab.Project
	var nextPage int
	var jobsWait sync.WaitGroup

	prjsChannel = make(chan *[]*gitlab.Project, gCli.Int("queue-workers"))

	// job responses collector:
	go func() {
		gLog.Debug().Msgf("there is %d jobs in queue", len(prjsChannel))
		defer gLog.Debug().Msg("jobs result collector has been finished")

		var ok bool
		var jobProjects *[]*gitlab.Project

		for {
			jobProjects, ok = <-prjsChannel
			gLog.Debug().Msg("there is new result from job, handling...")
			if !ok {
				return
			}

			projects = append(projects, *jobProjects...)
		}
	}()

	// job spawner:
	for i, group := range groups {
		gLog.Debug().Msgf("there are %d groups waiting for scaning; scan #%d", len(groups), i)
		rsp := responsePool.Get().(*gitlab.Response)

		for {

			// first call for totalPages variable get
			if rsp.TotalPages == 0 {
				if prjs, rsp, e = m.getProjectsFromPage(group.ID, rsp.NextPage); e == nil {
					projects = append(projects, prjs...)

					if rsp.NextPage == 0 {
						break
					}

					nextPage = rsp.NextPage
					gLog.Debug().Msgf("nextpage %d", rsp.NextPage)
				} else {
					gLog.Error().Err(e).Msg("There is abnraml result from Gitlab API")
					return
				}
			}

			if nextPage == rsp.TotalPages {
				break
			}

			// TODO ERROR HANDLING
			// async calls (jobs spawn)
			args := map[string]interface{}{
				"page":    nextPage,
				"group":   group.ID,
				"channel": prjsChannel,
				"done":    jobsWait.Done,
			}

			jobsWait.Add(1)
			gQueue <- &job{
				fn: func(payload map[string]interface{}) error {
					defer payload["done"].(func())()

					page, group := payload["page"].(int), payload["group"].(int)
					pChannel := payload["channel"].(chan *[]*gitlab.Project)

					gLog.Debug().Msgf("There is new job with page %d and group %d", page, group)

					prjs, _, e := m.getProjectsFromPage(group, page)
					if e != nil {
						gLog.Error().Err(e).Msg("There is abnormal result from Gitlab API")
						// TODO ADD ERROR MESSAGE HANDLING
						return e
					}

					gLog.Debug().Msg("trying to push projects in pipe")
					pChannel <- &prjs

					gLog.Debug().Msg("all done, job can be stopped now")
					return nil
				},
				args: args,
			}

			nextPage++
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

	gLog.Debug().Msg("all jobs are executed, close result pipeline")
	close(prjsChannel)

	return
}

// !!! --------------------------------------------------------
// TODO REMOVE SUBGROUPS PARSING, USE OPTIONS WITH SUBGROUPS
func (m *glClient) getProjectsFromPage(gid, page int) ([]*gitlab.Project, *gitlab.Response, error) {
	gLog.Debug().Msgf("Called with gid %d, page %d", gid, page)

	listOptions := gitlab.ListOptions{}
	if page != 0 {
		listOptions.Page = page
	}

	return m.instance.Groups.ListGroupProjects(gid, &gitlab.ListGroupProjectsOptions{
		ListOptions: listOptions,
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
				return
			}

			groups = append(groups, *jobGroups...)
		}
	}(collectorWait.Done)

	// job spawner:
	for nextPage := 1; nextPage != rsp.TotalPages && gCtx.Err() == nil; nextPage++ {
		if rsp.TotalPages == 0 {
			if grp, rsp, e = m.getGroupsFromPage(rsp.NextPage); e == nil {
				grp = m.getMatchedGroups(grp)
				grpsChannel <- &grp

				if rsp.NextPage == 0 {
					break
				}

				nextPage = rsp.NextPage
				gLog.Debug().Msgf("nextpage %d", rsp.NextPage)
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
			"done":    jobsWait.Done,
		}

		jobsWait.Add(1)
		gQueue <- &job{
			fn: func(payload map[string]interface{}) error {
				defer payload["done"].(func())()

				page := payload["page"].(int)
				pChannel := payload["channel"].(chan *[]*gitlab.Group)

				gLog.Debug().Msgf("There is new job with page %d", page)

				grps, _, e := m.getGroupsFromPage(page)
				if e != nil {
					gLog.Error().Err(e).Msg("There is abnormal result from Gitlab API")
					// TODO ADD ERROR MESSAGE HANDLING
					return e
				}

				grps = m.getMatchedGroups(grps)
				pChannel <- &grps

				gLog.Debug().Msg("all done, job can be stopped now")
				return nil
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

func (m *glClient) getInstanceGroups() (groups []*gitlab.Group, e error) {
	var grp []*gitlab.Group
	var rsp *gitlab.Response = &gitlab.Response{}

	for {
		if grp, rsp, e = m.getGroupsFromPage(rsp.NextPage); e == nil {
			groups = append(groups, grp...)

			if rsp.NextPage == 0 {
				break
			}
		} else {
			return
		}
	}

	if len(m.groupPrefix) != 0 {
		groups = m.getMatchedGroups(groups)
	}

	gLog.Debug().Msgf("There are %d top groups found with given search criteria", len(groups))

	for i := 0; i < len(groups); i++ {
		group := groups[i]

		for {
			if grp, rsp, e = m.getSubgroupsFromPage(group.ID, rsp.NextPage); e == nil {
				groups = append(groups, grp...)

				if rsp.NextPage == 0 {
					break
				}
			} else {
				return
			}
		}
	}

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

func (m *glClient) getSubgroupsFromPage(gid, page int) ([]*gitlab.Group, *gitlab.Response, error) {
	gLog.Debug().Msgf("Called with gid %d, page %d", gid, page)

	listOptions := gitlab.ListOptions{}
	if page != 0 {
		listOptions.Page = page
	}

	return m.instance.Groups.ListSubgroups(gid, &gitlab.ListSubgroupsOptions{
		ListOptions: listOptions,
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
