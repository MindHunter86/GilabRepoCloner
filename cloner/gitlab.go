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
	if groups, e = m.getInstanceGroups(0); e != nil {
		return
	}

	m.printGroups(groups)
	return
}

func (m *glClient) getInstanceGroups(page int) (groups []*gitlab.Group, err error) {
	var rsp *gitlab.Response
	if groups, rsp, err = m.instance.Groups.ListGroups(&gitlab.ListGroupsOptions{
		ListOptions: gitlab.ListOptions{
			Page: page,
		},
		AllAvailable: gitlab.Bool(true),
		TopLevelOnly: gitlab.Bool(true),
	}); err != nil {
		return
	}

	if rsp.StatusCode != http.StatusOK {
		gLog.Warn().Int("status_code", rsp.StatusCode).Msg("Threre is abnormal response code from gitlab instance! Please check logs.")
		return
	}

	if len(m.groupPrefix) != 0 {
		m.getMatchedGroups(groups)
	}

	if rsp.CurrentPage != rsp.TotalPages {
		if groups2, err := m.getInstanceGroups(rsp.NextPage); err == nil {
			groups = append(groups, groups2...)
		}
		return
	}

	//

	return
}

func (m *glClient) getMatchedGroups(groups []*gitlab.Group) {
	for i, group := range groups {
		if group.FullPath == m.groupPrefix {
			continue
		}

		groups[i] = nil
	}
}

func (m *glClient) getInstanceGroupTree() {

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

func (m *glClient) getGroupSubgroups() {

}

func (m *glClient) getGroupRepositories() {

}
