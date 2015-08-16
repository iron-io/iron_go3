// api provides common functionality for all the iron.io APIs
package api

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/iron-io/iron_go3/config"
)

type DefaultResponseBody struct {
	Msg string `json:"msg"`
}

type URL struct {
	URL      url.URL
	Settings config.Settings
}

var (
	debug            bool
	DebugOnErrors    bool
	DefaultCacheSize = 8192

	// HttpClient is the client used by iron_go to make each http request. It is exported in case
	// the client would like to modify it from the default behavior from http.DefaultClient.
	// This uses the DefaultTransport modified to enable TLS Session Client caching.
	HttpClient = &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			MaxIdleConnsPerHost: 512,
			TLSHandshakeTimeout: 10 * time.Second,
			TLSClientConfig: &tls.Config{
				ClientSessionCache: tls.NewLRUClientSessionCache(DefaultCacheSize),
			},
		},
	}
)

func dbg(v ...interface{}) {
	if debug {
		fmt.Fprintln(os.Stderr, v...)
	}
}

func init() {
	if os.Getenv("IRON_API_DEBUG") != "" {
		debug = true
		dbg("debugging of api enabled")
	}
}

func Action(cs config.Settings, prefix string, suffix ...string) *URL {
	parts := append([]string{prefix}, suffix...)
	return ActionEndpoint(cs, strings.Join(parts, "/"))
}

func ActionEndpoint(cs config.Settings, endpoint string) *URL {
	u := &URL{Settings: cs, URL: url.URL{}}
	u.URL.Scheme = cs.Scheme
	u.URL.Host = fmt.Sprintf("%s:%d", cs.Host, cs.Port)
	u.URL.Path = fmt.Sprintf("/%s/projects/%s/%s", cs.ApiVersion, cs.ProjectId, endpoint)
	return u
}

func VersionAction(cs config.Settings) *URL {
	u := &URL{Settings: cs, URL: url.URL{Scheme: cs.Scheme}}
	u.URL.Host = fmt.Sprintf("%s:%d", cs.Host, cs.Port)
	u.URL.Path = "/version"
	return u
}

func (u *URL) QueryAdd(key string, format string, value interface{}) *URL {
	query := u.URL.Query()
	query.Add(key, fmt.Sprintf(format, value))
	u.URL.RawQuery = query.Encode()
	return u
}

func (u *URL) Req(method string, in, out interface{}) (err error) {
	if in == nil {
		in = &struct{}{}
	}
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}

	response, err := u.req(method, data)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err == nil && out != nil {
		err = json.NewDecoder(response.Body).Decode(out)
		dbg("u:", u, "out:", fmt.Sprintf("%#v\n", out))
	}

	// throw it away
	io.Copy(ioutil.Discard, response.Body)
	return
}

var MaxRequestRetries = 5

func (u *URL) Request(method string, body io.Reader) (response *http.Response, err error) {
	bytes, err := ioutil.ReadAll(body)
	if err != nil {
		return
	}
	return u.req(method, bytes)
}

func (u *URL) req(method string, body []byte) (response *http.Response, err error) {
	request, err := http.NewRequest(method, u.URL.String(), nil)
	if err != nil {
		return nil, err
	}

	request.ContentLength = int64(len(body))
	request.Header.Set("Authorization", "OAuth "+u.Settings.Token)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "gzip/deflate")
	request.Header.Set("User-Agent", u.Settings.UserAgent)

	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	dbg("request:", fmt.Sprintf("%#v\n", request))

	for tries := 0; tries <= MaxRequestRetries; tries++ {
		request.Body = ioutil.NopCloser(bytes.NewReader(body)) // reads are only useful once
		response, err = HttpClient.Do(request)
		if err != nil {
			if response != nil && response.Body != nil {
				response.Body.Close()
			}
			if err == io.EOF {
				continue
			}
			return nil, err
		}

		if response.StatusCode == http.StatusServiceUnavailable {
			delay := (tries + 1) * 10 // smooth out delays from 0-2
			time.Sleep(time.Duration(delay*delay) * time.Millisecond)
			continue
		}

		break
	}

	// DumpResponse(response)
	if err = ResponseAsError(response); err != nil {
		return nil, err
	}

	return
}

func DumpRequest(req *http.Request) {
	out, err := httputil.DumpRequestOut(req, true)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%q\n", out)
}

func DumpResponse(response *http.Response) {
	out, err := httputil.DumpResponse(response, true)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%q\n", out)
}

var HTTPErrorDescriptions = map[int]string{
	http.StatusUnauthorized:     "The OAuth token is either not provided or invalid",
	http.StatusNotFound:         "The resource, project, or endpoint being requested doesn't exist.",
	http.StatusMethodNotAllowed: "This endpoint doesn't support that particular verb",
	http.StatusNotAcceptable:    "Required fields are missing",
}

func ResponseAsError(response *http.Response) HTTPResponseError {
	if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusCreated {
		return nil
	}

	if response.Body != nil {
		defer response.Body.Close()
	}

	desc, found := HTTPErrorDescriptions[response.StatusCode]
	if found {
		return resErr{response: response, error: response.Status + ": " + desc}
	}

	var out DefaultResponseBody
	err := json.NewDecoder(response.Body).Decode(&out)
	if err != nil {
		return resErr{response: response, error: fmt.Sprint(response.Status, ": ", err.Error())}
	}
	if out.Msg != "" {
		return resErr{response: response, error: fmt.Sprint(response.Status, ": ", out.Msg)}
	}

	return resErr{response: response, error: response.Status + ": Unknown API Response"}
}

type HTTPResponseError interface {
	Error() string
	Response() *http.Response
}

type resErr struct {
	error    string
	response *http.Response
}

func (h resErr) Error() string            { return h.error }
func (h resErr) Response() *http.Response { return h.response }
func (h resErr) StatusCode() int          { return h.response.StatusCode }
