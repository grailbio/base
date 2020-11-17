package main

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	auth "k8s.io/api/authentication/v1"
	client "k8s.io/client-go/kubernetes/typed/authentication/v1"
	"k8s.io/client-go/rest"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	awssigner "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/eks"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/grailbio/base/common/log"
	"github.com/grailbio/base/errors"
	v23context "v.io/v23/context"
	"v.io/v23/rpc"
	"v.io/v23/security"
)

type k8sBlesser struct {
	session            *session.Session
	role               string
	expirationInterval time.Duration
	awsAccountIDs      []string
	awsRegions         []string
}

func newK8sBlesser(session *session.Session, expiration time.Duration, role string, awsAccountIDs []string, awsRegions []string) *k8sBlesser {
	return &k8sBlesser{
		session:            session,
		role:               role,
		expirationInterval: expiration,
		awsAccountIDs:      awsAccountIDs,
		awsRegions:         awsRegions,
	}
}

func (blesser *k8sBlesser) BlessK8s(ctx *v23context.T, call rpc.ServerCall, caCrt string, namespace string, k8sSvcAcctToken string, region string) (security.Blessings, error) {
	// TODO(noah): If performance becomes an issue, populate allow-list of clusters on ticket-server startup.
	var cluster *eks.Cluster
	caCrtData := base64.StdEncoding.EncodeToString([]byte(caCrt))
	for _, c := range blesser.getClusters(ctx, region) {
		if caCrtData == *c.CertificateAuthority.Data {
			cluster = c
			break
		}
	}
	if cluster == nil {
		return security.Blessings{}, errors.New("CA certificate does not match any cluster")
	}

	// Set STS regional endpoint from "legacy" to "regional"
	blesser.session.Config.STSRegionalEndpoint = endpoints.RegionalSTSEndpoint
	svc := sts.New(blesser.session)
	emptyBody := strings.NewReader("")
	signer := awssigner.NewSigner(blesser.session.Config.Credentials)
	req, _ := http.NewRequest("GET", fmt.Sprintf("%s/?Action=GetCallerIdentity&Version=2011-06-15", svc.Client.Endpoint), nil)
	req.Header.Add("x-k8s-aws-id", *cluster.Name)
	_, err := signer.Presign(req, emptyBody, "sts", region, 60*time.Second, time.Now())
	if err != nil {
		return security.Blessings{}, errors.E(err, "unable to presign request for STS credentials")
	}
	bearerToken := fmt.Sprintf("k8s-aws-v1.%s", strings.TrimRight(base64.StdEncoding.EncodeToString([]byte(req.URL.String())), "="))

	tlsConfig := rest.TLSClientConfig{CAData: []byte(caCrt)}
	config := rest.Config{
		Host:            *cluster.Endpoint,
		BearerToken:     bearerToken,
		TLSClientConfig: tlsConfig,
	}
	authV1Client := client.NewForConfigOrDie(&config)

	tr := auth.TokenReview{
		Spec: auth.TokenReviewSpec{
			Token: k8sSvcAcctToken,
		},
	}
	trResp, err := authV1Client.TokenReviews().Create(&tr)
	if err != nil {
		return security.Blessings{}, errors.E(err, "unable to create tokenreview")
	}

	if !trResp.Status.Authenticated {
		return security.Blessings{}, errors.New("requestToken authentication failed")
	}

	// Username is of format: system:serviceaccount:(NAMESPACE):(SERVICEACCOUNT)
	parts := strings.Split(trResp.Status.User.Username, ":")
	if len(parts) != 4 {
		return security.Blessings{}, errors.New("username does not match format system:serviceaccount:(NAMESPACE):(SERVICEACCOUNT)")
	}
	if namespace != parts[2] {
		return security.Blessings{}, errors.New("namespace does not match")
	}

	// Blessing is of format: v23.grail.com:k8s:<aws_account_id>:<cluster_name>:<service_account_name>
	serviceAccountName := parts[3]
	accountID, err := getAccountID(cluster)
	if err != nil {
		return security.Blessings{}, errors.E(err, "unable to parse account id from cluster")
	}
	ext := fmt.Sprintf("k8s:%s:%s:%s", accountID, *cluster.Name, serviceAccountName)
	securityCall := call.Security()
	if securityCall.LocalPrincipal() == nil {
		return security.Blessings{}, errors.New("server misconfiguration: no authentication happened")
	}

	pubKey := securityCall.RemoteBlessings().PublicKey()
	caveat, err := security.NewExpiryCaveat(time.Now().Add(blesser.expirationInterval))
	if err != nil {
		return security.Blessings{}, err
	}
	return securityCall.LocalPrincipal().Bless(pubKey, securityCall.LocalBlessings(), ext, caveat)
}

// Get allow-list of clusters and their info.
func (blesser *k8sBlesser) getClusters(ctx *v23context.T, region string) []*eks.Cluster {
	var clusters []*eks.Cluster
	for _, r := range blesser.awsRegions {
		if r == region {
			for _, id := range blesser.awsAccountIDs {
				roleARN := fmt.Sprintf("arn:aws:iam::%s:role/%s", id, blesser.role)
				config := aws.Config{
					Credentials: stscreds.NewCredentials(blesser.session, roleARN),
					Region:      &region,
				}
				svc := eks.New(blesser.session, &config)
				input := &eks.ListClustersInput{}
				listClusterOutput, err := svc.ListClusters(input)
				if err != nil {
					log.Error(ctx, "Unable to fetch list of clusters.", "roleARN", roleARN, "region", region)
				}
				for _, name := range listClusterOutput.Clusters {
					describeClusterInput := eks.DescribeClusterInput{Name: name}
					describeClusterOutput, err := svc.DescribeCluster(&describeClusterInput)
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

func getAccountID(cluster *eks.Cluster) (string, error) {
	arn, err := arn.Parse(*cluster.Arn)
	if err != nil {
		return "", err
	}
	return arn.AccountID, nil
}
