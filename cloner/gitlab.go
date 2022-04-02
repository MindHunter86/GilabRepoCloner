package cloner

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"os"
	"strings"

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
	if groups, e = m.getInstanceGroups(); e != nil {
		return
	}

	m.printGroups(groups)
	return
}

func (m *glClient) printRepositoriesAction() (e error) {
	groups, projects := []*gitlab.Group{}, []*gitlab.Project{}

	if groups, e = m.getInstanceGroups(); e != nil {
		return
	}

	if projects, e = m.getInstanceProjects(groups); e != nil {
		return
	}

	m.printProjects(projects)
	return
}

func (m *glClient) getInstanceProjects(groups []*gitlab.Group) (projects []*gitlab.Project, e error) {
	prjs, rsp := []*gitlab.Project{}, &gitlab.Response{}

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

func (m *glClient) getInstanceGroups() (groups []*gitlab.Group, e error) {
	grp, rsp := []*gitlab.Group{}, &gitlab.Response{}

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
