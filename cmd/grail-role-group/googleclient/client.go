package googleclient

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/grailbio/base/cmdutil"
	"github.com/grailbio/base/web/webutil"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	goauth2 "google.golang.org/api/oauth2/v2"
	"v.io/x/lib/vlog"
)

// Options describes various options that can be used to create the client.
type Options struct {
	ClientID     string
	ClientSecret string

	// Scopes indicates what scope to request.
	Scopes []string

	// AccessType indicates what type of access is desireed. The two possible
	// options are oauth2.AccessTypeOnline and oauth2.AccessTypeOffline.
	AccessType oauth2.AuthCodeOption

	// ConfigFile indicates where to look for and save the credentials. On Linux
	// this is something like ~/.config/grail-role-group/credentials.json.
	ConfigFile string

	// OpenBrowser indicates that a browser should be open to obtain the proper
	// credentials if they are missing.
	OpenBrowser bool
}

// saveToken makes a best-effort attempt to save the token.
func saveToken(configFile string, token *oauth2.Token) {
	os.MkdirAll(filepath.Dir(configFile), 0700)
	f, err := os.OpenFile(configFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		vlog.Info(err)
		return
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(token); err != nil {
		vlog.Info(err)
	}
}

func loadToken(configFile string) *oauth2.Token {
	f, err := os.Open(configFile)
	if err != nil {
		vlog.Info(err)
		return nil
	}
	t := &oauth2.Token{}
	if err := json.NewDecoder(f).Decode(t); err != nil {
		vlog.Info(err)
		return nil
	}
	defer f.Close()
	return t
}

// New returns a new http.Client suitable for passing to the New functions from
// the packages under google.golang.org/api/. An interactive OAuth flow is
// performed if the credentials don't exist in the config file. An attempt to
// be open a web browser will done if opts.OpenBrowser is true.
//
// TODO(razvanm): add support for refresh tokens.
// TODO(razvanm): add support for Application Default Credentials.
func New(opts Options) (*http.Client, error) {
	token := loadToken(opts.ConfigFile)
	if token != nil {
		// Check the validity of the access token.
		config := &oauth2.Config{}
		service, err := goauth2.New(http.DefaultClient)
		if err != nil {
			vlog.Info(err)
		} else {
			_, err = service.Tokeninfo().AccessToken(token.AccessToken).Do()
			if err != nil {
				vlog.Info(err)
			} else {
				return config.Client(context.Background(), token), nil
			}
		}
	}

	stateBytes := make([]byte, 16)
	if _, err := rand.Read(stateBytes); err != nil {
		return nil, err
	}
	state := hex.EncodeToString(stateBytes)

	code := ""
	wg := sync.WaitGroup{}
	wg.Add(1)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			return
		}
		if got, want := r.FormValue("state"), state; got != want {
			cmdutil.Fatalf("Bad state: got %q, want %q", got, want)
		}
		code = r.FormValue("code")
		w.Header().Set("Content-Type", "text/html")
		// JavaScript only allows closing windows/tab that were open via JavaScript.
		fmt.Fprintf(w, `<html><body>Code received. Please close this tab/window.</body></html>`)
		wg.Done()
	})

	ln, err := net.Listen("tcp", "localhost:")
	if err != nil {
		return nil, err
	}
	vlog.Infof("listening: %v\n", ln.Addr().String())
	port := strings.Split(ln.Addr().String(), ":")[1]
	server := http.Server{Addr: "localhost:"}
	go server.Serve(ln.(*net.TCPListener))

	config := &oauth2.Config{
		ClientID:     opts.ClientID,
		ClientSecret: opts.ClientSecret,
		Scopes:       opts.Scopes,
		RedirectURL:  fmt.Sprintf("http://localhost:%s", port),
		Endpoint:     google.Endpoint,
	}

	url := config.AuthCodeURL(state, opts.AccessType)

	if opts.OpenBrowser {
		fmt.Printf("Opening %q...\n", url)
		if webutil.StartBrowser(url) {
			wg.Wait()
			server.Shutdown(context.Background())
		} else {
			opts.OpenBrowser = false
		}
	}

	if !opts.OpenBrowser {
		config.RedirectURL = "urn:ietf:wg:oauth:2.0:oob"
		fmt.Printf("The attempt to automatically open a browser failed. Please open the following link in your browse:\n\n\t%s\n\n", url)
		fmt.Printf("Paste the received code and then press enter: ")
		fmt.Scanf("%s", &code)
		fmt.Println("")
	}

	vlog.VI(1).Infof("code: %+v", code)
	token, err = config.Exchange(oauth2.NoContext, code)
	if err != nil {
		return nil, err
	}

	saveToken(opts.ConfigFile, token)

	return config.Client(context.Background(), token), nil
}
