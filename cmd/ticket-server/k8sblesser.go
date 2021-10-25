package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	auth "k8s.io/api/authentication/v1"
	client "k8s.io/client-go/kubernetes/typed/authentication/v1"
	rest "k8s.io/client-go/rest"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	awsclient "github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	awssession "github.com/aws/aws-sdk-go/aws/session"
	awssigner "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/eks"
	"github.com/aws/aws-sdk-go/service/sts"

	"github.com/grailbio/base/common/log"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/security/identity"

	v23context "v.io/v23/context"
	"v.io/v23/rpc"
	"v.io/v23/security"
)

// AWSSessionWrapper is a composition struture that wraps the
// aws session element and returns values that can be more
// easily mocked for testing purposes. SessionI interface
// is passed into the wrapper so that functionality within the
// session function can
type AWSSessionWrapper struct {
	session SessionI
}

// AWSSessionWrapperI provides a means to mock an aws client session.
type AWSSessionWrapperI interface {
	GetAuthV1Client(ctx context.Context, headers map[string]string, caCrt string, region string, endpoint string) (client.AuthenticationV1Interface, error)
	ListEKSClusters(input *eks.ListClustersInput, roleARN string, region string) (*eks.ListClustersOutput, error)
	DescribeEKSCluster(input *eks.DescribeClusterInput, roleARN string, region string) (*eks.DescribeClusterOutput, error)
}

// newSessionWrapper generates an AWSSessionWrapper that contains
// an awsSession.Session struct and provides multiple mockable interfaces
// for interacting with aws and its remote data.
func newSessionWrapper(session SessionI) *AWSSessionWrapper {
	// in order to update the sessionI config we must cast it as an awssession.Session struct
	newSession := session.(*awssession.Session)
	newSession.Config.STSRegionalEndpoint = endpoints.RegionalSTSEndpoint

	return &AWSSessionWrapper{session: newSession}
}

// ListEKSClusters provides a mockable interface for AWS sessions to
// obtain and iterate over a list of available EKS clusters with the
// provided input configuration
func (w *AWSSessionWrapper) ListEKSClusters(input *eks.ListClustersInput, roleARN string, region string) (*eks.ListClustersOutput, error) {
	config := aws.Config{
		Credentials: stscreds.NewCredentials(w.session, roleARN), // w.session.GetStsCreds(roleARN),
		Region:      &region,
	}

	svc := eks.New(w.session, &config)
	return svc.ListClusters(input)
}

// DescribeEKSCluster provides a mockable interface for AWS sessions to
// obtain information regarding a specific EKS cluster with the
// provided input configuration
func (w *AWSSessionWrapper) DescribeEKSCluster(input *eks.DescribeClusterInput, roleARN string, region string) (*eks.DescribeClusterOutput, error) {
	config := aws.Config{
		Credentials: stscreds.NewCredentials(w.session, roleARN), // w.session.GetStsCreds(roleARN),
		Region:      &region,
	}
	svc := eks.New(w.session, &config)
	return svc.DescribeCluster(input)
}

// GetAuthV1Client provides a mockable interface for returning an AWS auth client
func (w *AWSSessionWrapper) GetAuthV1Client(ctx context.Context, headers map[string]string, caCrt string, region string, endpoint string) (client.AuthenticationV1Interface, error) {
	var (
		err          error
		authV1Client *client.AuthenticationV1Client
	)
	svc := sts.New(w.session)
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/?Action=GetCallerIdentity&Version=2011-06-15", svc.Client.Endpoint), nil)

	for key, header := range headers {
		req.Header.Add(key, header)
	}

	var sessionInterface = w.session
	var credentials = sessionInterface.(*awssession.Session).Config.Credentials

	signer := awssigner.NewSigner(credentials)
	emptyBody := strings.NewReader("")
	_, err = signer.Presign(req, emptyBody, "sts", region, 60*time.Second, time.Now())

	log.Debug(ctx, "Request was built and presigned", "req", req)

	if err != nil {
		return authV1Client, errors.E(err, "unable to presign request for STS credentials")
	}

	bearerToken := fmt.Sprintf("k8s-aws-v1.%s", strings.TrimRight(base64.StdEncoding.EncodeToString([]byte(req.URL.String())), "="))

	log.Debug(ctx, "Bearer token generated", "bearerToken", bearerToken, "url", req.URL.String())

	tlsConfig := rest.TLSClientConfig{CAData: []byte(caCrt)}
	config := rest.Config{
		Host:            endpoint,
		BearerToken:     bearerToken,
		TLSClientConfig: tlsConfig,
	}

	return client.NewForConfigOrDie(&config), err
}

// SessionI interface provides a mockable interface for session data
type SessionI interface {
	awsclient.ConfigProvider
}

// V23 Blesser utility for generating blessings for k8s cluster principals. Implements
// interface K8sBlesserServerStubMethods, which requires a BlessK8s method.
// Stores awsConn information in addition to the v23 session and blessing expiration intervals.
// Mock this by creating a separate implementation of K8sBlesserServerStubMethods interface.
type k8sBlesser struct {
	identity.K8sBlesserServerMethods
	sessionWrapper     AWSSessionWrapperI
	expirationInterval time.Duration
	awsConn            *awsConn
}

func newK8sBlesser(sessionWrapper AWSSessionWrapperI, expiration time.Duration, role string, awsAccountIDs []string, awsRegions []string) *k8sBlesser {
	return &k8sBlesser{
		sessionWrapper:     sessionWrapper,
		expirationInterval: expiration,
		awsConn:            newAwsConn(sessionWrapper, role, awsRegions, awsAccountIDs),
	}
}

// BlessK8s uses the awsConn and k8sConn structs as well as the CreateK8sExtension func
// in order to create a blessing for a k8s principle. It acts as an entrypoint that does not
// perform any important logic on its own.
func (blesser *k8sBlesser) BlessK8s(ctx *v23context.T, call rpc.ServerCall, caCrt string, namespace string, k8sSvcAcctToken string, region string) (security.Blessings, error) {
	log.Info(ctx, "bless K8s request", "namespace", namespace, "region", region, "remoteAddr", call.RemoteEndpoint().Address)
	var (
		nullBlessings security.Blessings = security.Blessings{}
		cluster       *eks.Cluster
		err           error
	)

	// establish security call
	securityCall := call.Security()
	if securityCall.LocalPrincipal() == nil {
		return nullBlessings, errors.New("server misconfiguration: no authentication happened")
	}

	// establish caveat
	caveat, err := security.NewExpiryCaveat(time.Now().Add(blesser.expirationInterval))
	if err != nil {
		return nullBlessings, errors.E(err, "unable to presign request for STS credentials")
	}

	// next, we are ready to isolate a desired cluster by enumerating existing eks clusters in a region and matching the caCrt
	cluster, err = blesser.awsConn.GetEKSCluster(ctx, region, caCrt)
	if err != nil {
		return nullBlessings, err
	}

	// now we can establish the k8s cluster obj because we know the cluster and can connect to it.
	k8sConn := newK8sConn(blesser.sessionWrapper, cluster, region, caCrt, k8sSvcAcctToken)

	// obtain username from cluster connection
	username, err := k8sConn.GetK8sUsername(ctx)
	if err != nil {
		return nullBlessings, err
	}

	// create an extension based on the namespace and username
	extension, err := CreateK8sExtension(ctx, cluster, username, namespace)
	if err != nil {
		return nullBlessings, err
	}

	// lastly we perform the blessing using the generated k8s extension
	return call.Security().LocalPrincipal().Bless(securityCall.RemoteBlessings().PublicKey(), securityCall.LocalBlessings(), extension, caveat)
}

// Provides an interface for gather aws data using the context, region, caCrt which can be mocked for testing.
type awsConn struct {
	role           string
	regions        []string
	accountIDs     []string
	sessionWrapper AWSSessionWrapperI
}

// Creates a new AWS Connect object that can be used to obtain data about AWS, EKS, etc.
func newAwsConn(sessionWrapper AWSSessionWrapperI, role string, regions []string, accountIDs []string) *awsConn {
	return &awsConn{
		sessionWrapper: sessionWrapper,
		role:           role,
		regions:        regions,
		accountIDs:     accountIDs,
	}
}

// Interface for mocking awsConn.
type awsConnI interface {
	GetEKSCluster(caCrt string) (*eks.Cluster, error)
	GetClusters(ctx *v23context.T, region string) []*eks.Cluster
}

// Gets an EKS Cluster with Matching AWS region and caCrt.
func (conn *awsConn) GetEKSCluster(ctx *v23context.T, region string, caCrt string) (*eks.Cluster, error) {
	var (
		cluster *eks.Cluster
		err     error
	)

	caCrtData := base64.StdEncoding.EncodeToString([]byte(caCrt))
	// TODO(noah): If performance becomes an issue, populate allow-list of clusters on ticket-server startup.
	for _, c := range conn.GetClusters(ctx, region) {
		if caCrtData == *c.CertificateAuthority.Data {
			cluster = c
			break
		}
	}
	if cluster == nil {
		err = errors.New("CA certificate does not match any cluster")
	}
	return cluster, err
}

// Gets all EKS clusters in a given AWS region.
func (conn *awsConn) GetClusters(ctx *v23context.T, region string) []*eks.Cluster {
	var clusters []*eks.Cluster
	for _, r := range conn.regions {
		if r == region {
			for _, id := range conn.accountIDs {
				roleARN := fmt.Sprintf("arn:aws:iam::%s:role/%s", id, conn.role)
				listClusterOutput, err := conn.sessionWrapper.ListEKSClusters(&eks.ListClustersInput{}, roleARN, region)
				if err != nil {
					log.Error(ctx, "Unable to fetch list of clusters.", "roleARN", roleARN, "region", region)
				}
				for _, name := range listClusterOutput.Clusters {
					describeClusterOutput, err := conn.sessionWrapper.DescribeEKSCluster(&eks.DescribeClusterInput{Name: name}, roleARN, region)
					if err != nil {
						log.Error(ctx, "Unable to describe cluster.", "clusterName", *name)
					}
					clusters = append(clusters, describeClusterOutput.Cluster)
				}
			}
		}
	}
	return clusters
}

// Defines connection parameters to a k8s cluster and can connect and return data. Isolated as an interface so complex http calls can be mocked for testing.
type k8sConn struct {
	cluster        *eks.Cluster
	namespace      string
	region         string
	caCrt          string
	svcAcctToken   string
	sessionWrapper AWSSessionWrapperI
}

// Creates a new k8s connection object that can be used to connect to the k8s cluster and obtain relevant data.
func newK8sConn(sessionWrapper AWSSessionWrapperI, cluster *eks.Cluster, region string, caCrt string, svcAcctToken string) *k8sConn {
	return &k8sConn{
		sessionWrapper: sessionWrapper,
		cluster:        cluster,
		region:         region,
		caCrt:          caCrt,
		svcAcctToken:   svcAcctToken,
	}
}

// An interface for mocking k8sConn struct.
type k8sConnI interface {
	GetK8sUsername(ctx context.Context) (string, error)
}

func (conn *k8sConn) GetK8sUsername(ctx context.Context) (string, error) {
	var (
		username string
		err      error
	)

	//svc := sts.New(conn.session)

	var headers = make(map[string]string)
	headers["x-k8s-aws-id"] = *conn.cluster.Name

	authV1Client, err := conn.sessionWrapper.GetAuthV1Client(ctx, headers, conn.caCrt, conn.region, *conn.cluster.Endpoint)
	if err != nil {
		return username, err
	}

	log.Debug(ctx, "AuthV1Client retrieved", "caCrt", conn.caCrt, "region", conn.region, "endpoint", *conn.cluster.Endpoint)

	tr := auth.TokenReview{
		Spec: auth.TokenReviewSpec{
			Token: conn.svcAcctToken,
		},
	}

	log.Debug(ctx, "K8s Service account token configured for tokenReview request", "token", conn.svcAcctToken)

	trResp, err := authV1Client.TokenReviews().Create(&tr)
	username = trResp.Status.User.Username

	if err != nil {
		err = errors.E(err, "unable to create tokenreview")
	} else if !trResp.Status.Authenticated {
		err = errors.New("requestToken authentication failed")
	}

	return username, err
}

// CreateK8sExtension evaluates EKS Cluster configuration and tagging to produce a v23 Blessing extension.
func CreateK8sExtension(ctx context.Context, cluster *eks.Cluster, username, namespace string) (string, error) {
	var (
		extension          string
		err                error
		clusterNameFromTag string
		clusterModeFromTag string
	)

	arn, err := arn.Parse(*cluster.Arn)
	if err != nil {
		return extension, err
	}

	// Username is of format: system:serviceaccount:(NAMESPACE):(SERVICEACCOUNT)
	usernameSet := strings.Split(username, ":")

	if len(usernameSet) != 4 {
		return extension, errors.New("username does not match format system:serviceaccount:(NAMESPACE):(SERVICEACCOUNT)")
	} else if namespace != usernameSet[2] {
		return extension, errors.New("namespace does not match")
	}

	if val, ok := cluster.Tags["ClusterName"]; ok {
		clusterNameFromTag = *val
	}

	if val, ok := cluster.Tags["ClusterMode"]; ok {
		clusterModeFromTag = strings.ToLower(*val)
	}

	if clusterNameFromTag != "" && clusterModeFromTag != "" {
		extension = fmt.Sprintf("k8s:%s:%s:%s:%s", arn.AccountID, clusterNameFromTag, usernameSet[3], clusterModeFromTag)
		log.Debug(ctx, "Using k8s cluster a/b extension generation.", "extension", extension)
	} else {
		extension = fmt.Sprintf("k8s:%s:%s:%s", arn.AccountID, *cluster.Name, usernameSet[3])
		log.Debug(ctx, "Using standard k8s extension generation.", "extension", extension)
	}

	return extension, err
}
