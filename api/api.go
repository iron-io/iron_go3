// api provides common functionality for all the iron.io APIs
package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/iron-io/iron_go3/config"
	"log"
)

type DefaultResponseBody struct {
	Msg string `json:"msg"`
}

type URL struct {
	URL      url.URL
	Settings config.Settings
}

var (
	Debug bool
)

func dbg(v ...interface{}) {
	if Debug {
		fmt.Fprintln(os.Stderr, v...)
	}
}

func init() {
	if os.Getenv("IRON_API_DEBUG") != "" {
		Debug = true
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

func (u *URL) Req(method string, in, out interface{}) error {
	var reqBody io.Reader
	if in == nil {
		in = &map[string]string{}
	}
	data, err := json.Marshal(in)
	if err != nil {
		return err
	}
	reqBody = bytes.NewBuffer(data)
	dbg("request body:", in)

	response, err := u.Request(method, reqBody)
	if response != nil {
		defer closeResponse(response)
	}
	if err != nil {
		dbg("ERROR!", err, err.Error())
		return err
	}
	dbg("response:", response.Body)
	if err == nil {
		if out != nil {
			err = json.NewDecoder(response.Body).Decode(out)
			if err != nil {
				return err
			}
			dbg("u:", u, "out:", fmt.Sprintf("%#v\n", out))
		} else {
			// throw it away
			io.Copy(ioutil.Discard, response.Body)
		}
	}
	return nil
}

var MaxRequestRetries = 5

func (u *URL) Request(method string, body io.Reader) (response *http.Response, err error) {
	var bodyBytes []byte
	if body == nil {
		bodyBytes = []byte{}
	} else {
		bodyBytes, err = ioutil.ReadAll(body)
		if err != nil {
			return nil, err
		}
	}

	request, err := http.NewRequest(method, u.URL.String(), nil)
	if err != nil {
		return nil, err
	}

	request.Header.Set("Authorization", "OAuth "+u.Settings.Token)
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Accept-Encoding", "gzip/deflate")
	request.Header.Set("User-Agent", u.Settings.UserAgent)

	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	dbg("URL:", request.URL.String())
	dbg("request:", fmt.Sprintf("%#v\n", request))

	for tries := 0; tries <= MaxRequestRetries; tries++ {
		request.Body = ioutil.NopCloser(bytes.NewBuffer(bodyBytes))
		response, err = http.DefaultClient.Do(request)
		if err != nil {
			if err == io.EOF {
				continue
			}
			return
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
		return
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
	if response.StatusCode == http.StatusOK {
		return nil
	}

	defer closeResponse(response)

	//	desc, found := HTTPErrorDescriptions[response.StatusCode]
	//	if found {
	//		return resErr{response: response, error: response.Status + ": " + desc}
	//	}

	out := map[string]interface{}{}
	err := json.NewDecoder(response.Body).Decode(&out)
	if err != nil {
		return resErr{response: response, error: fmt.Sprint(response.Status, ": ", err.Error())}
	}
	if msg, ok := out["msg"]; ok {
		return resErr{response: response, error: fmt.Sprint(response.Status, ": ", msg)}
	}

	return resErr{response: response, error: response.Status + ": Unknown API Response"}
}

func closeResponse(response *http.Response) error {
	// ensure we read the entire body
	bs, err2 := ioutil.ReadAll(response.Body)
	if err2 != nil {
		log.Println("Error during ReadAll!!", err2)
	}
	if len(bs) > 0 {
		log.Println("Had to read some bytes, not good!", bs, string(bs))
	}
	return response.Body.Close()
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
