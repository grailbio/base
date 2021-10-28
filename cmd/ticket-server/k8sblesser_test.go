package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/eks"
	ticketServerUtil "github.com/grailbio/base/cmd/ticket-server/testutil"
	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/security/identity"
	"github.com/grailbio/base/vcontext"
	"github.com/grailbio/testutil"

	assert "github.com/stretchr/testify/assert"

	auth "k8s.io/api/authentication/v1"
	client "k8s.io/client-go/kubernetes/typed/authentication/v1"
	rest "k8s.io/client-go/rest"

	"v.io/v23/naming"
	"v.io/x/ref"
)

type FakeAWSSession struct {
}

type FakeAuthV1Client struct {
	client.AuthenticationV1Interface
	RESTClientReturn   rest.Interface
	TokenReviewsReturn client.TokenReviewInterface
}

func (w *FakeAuthV1Client) RESTClient() rest.Interface {
	return w.RESTClientReturn
}

func (w *FakeAuthV1Client) TokenReviews() client.TokenReviewInterface {
	return w.TokenReviewsReturn
}

type FakeTokenReviews struct {
	client.TokenReviewInterface
	TokenReviewReturn *auth.TokenReview
}

func (t *FakeTokenReviews) Create(*auth.TokenReview) (*auth.TokenReview, error) {
	var err error
	return t.TokenReviewReturn, err
}

// FakeAWSSessionWrapper mocks the session wrapper used to isolate
type FakeAWSSessionWrapper struct {
	session               *FakeAWSSession
	GetAuthV1ClientReturn client.AuthenticationV1Interface
	ListEKSClustersReturn *eks.ListClustersOutput
	AllEKSClusters        map[string]*eks.DescribeClusterOutput
}

func (w *FakeAWSSessionWrapper) DescribeEKSCluster(input *eks.DescribeClusterInput, roleARN string, region string) (*eks.DescribeClusterOutput, error) {
	var err error
	return w.AllEKSClusters[*input.Name], err
}

func (w *FakeAWSSessionWrapper) GetAuthV1Client(ctx context.Context, headers map[string]string, caCrt string, region string, endpoint string) (client.AuthenticationV1Interface, error) {
	var err error
	return w.GetAuthV1ClientReturn, err
}

func (w *FakeAWSSessionWrapper) ListEKSClusters(input *eks.ListClustersInput, roleARN string, region string) (*eks.ListClustersOutput, error) {
	var err error
	return w.ListEKSClustersReturn, err
}

// FakeContext mocks contexts so that we can pass them in to simulate logging, etc
type FakeContext struct {
	context.Context
}

// required to simulate logging.
func (c *FakeContext) Value(key interface{}) interface{} {
	return nil
}

// ClusterHelper generates all the cluster attributes used in a test
type ClusterHelper struct {
	Name          string
	Arn           string
	Crt           string
	CrtEnc        string
	RoleARN       string
	Endpoint      string
	Cluster       *eks.Cluster
	ClusterOutput *eks.DescribeClusterOutput
}

func newClusterHelper(name, acctNum, crt, roleARN, region string, tags map[string]*string) *ClusterHelper {
	fakeAccountName := "ACCTNAMEFOR" + name

	ch := ClusterHelper{
		Name:     name,
		Arn:      "arn:aws:iam::" + acctNum + ":role/" + name,
		Crt:      crt,
		CrtEnc:   base64.StdEncoding.EncodeToString([]byte(crt)),
		RoleARN:  roleARN,
		Endpoint: "https://" + fakeAccountName + ".sk1." + region + ".eks.amazonaws.com",
	}

	ch.Cluster = &eks.Cluster{
		Name:     &ch.Name,
		RoleArn:  &ch.RoleARN,
		Endpoint: &ch.Endpoint,
		Tags:     tags,
		Arn:      &ch.Arn,
		CertificateAuthority: &eks.Certificate{
			Data: &ch.CrtEnc,
		},
	}

	ch.ClusterOutput = &eks.DescribeClusterOutput{
		Cluster: ch.Cluster,
	}

	return &ch
}

// Note: we cannot test
func TestK8sBlesser(t *testing.T) {
	emptyTags := make(map[string]*string)
	randomTag := "test"
	emptyTags["RandomTag"] = &randomTag
	acctNum := "111111111111"

	ctx := vcontext.Background()
	assert.NoError(t, ref.EnvClearCredentials())

	t.Run("init", func(t *testing.T) {
		fakeSessionWrapper := &FakeAWSSessionWrapper{session: &FakeAWSSession{}}
		accountIDs := []string{"abc123456"}
		awsRegions := []string{"us-west-2"}
		testRole := "test-role"
		compareAWSConn := newAwsConn(fakeSessionWrapper, testRole, awsRegions, accountIDs)
		blesser := newK8sBlesser(fakeSessionWrapper, time.Hour, testRole, accountIDs, awsRegions)

		// test that awsConn was configured
		assert.Equal(t, blesser.awsConn, compareAWSConn)
	})

	t.Run("awsConn", func(t *testing.T) {
		fakeSessionWrapper := &FakeAWSSessionWrapper{session: &FakeAWSSession{}}
		accountIDs := []string{acctNum}
		awsRegions := []string{"us-west-2"}
		testRole := "test-role"
		testRegion := "us-west-2"
		wantCluster := newClusterHelper("test-cluster", acctNum, "fake-crt", testRole, testRegion, emptyTags)
		otherCluster1 := newClusterHelper("other-cluster1", acctNum, "other-crt1", testRole, testRegion, emptyTags)
		otherCluster2 := newClusterHelper("other-cluster2", acctNum, "other-crt2", "another-role", testRegion, emptyTags)

		clusters := []string{wantCluster.Name, otherCluster1.Name, otherCluster2.Name}
		var clusterOutputs = make(map[string]*eks.DescribeClusterOutput)
		clusterOutputs[wantCluster.Name] = wantCluster.ClusterOutput
		clusterOutputs[otherCluster1.Name] = otherCluster1.ClusterOutput
		clusterOutputs[otherCluster2.Name] = otherCluster2.ClusterOutput

		clusterPtrs := []*string{}
		for i := range clusters {
			clusterPtrs = append(clusterPtrs, &clusters[i])
		}

		fakeSessionWrapper.ListEKSClustersReturn = &eks.ListClustersOutput{
			Clusters: clusterPtrs,
		}

		fakeSessionWrapper.AllEKSClusters = clusterOutputs

		assert.NoError(t, ref.EnvClearCredentials())

		blesser := newK8sBlesser(fakeSessionWrapper, time.Hour, testRole, accountIDs, awsRegions)

		clustersOutput := blesser.awsConn.GetClusters(ctx, testRegion)
		assert.Equal(t, clustersOutput, []*eks.Cluster{wantCluster.Cluster, otherCluster1.Cluster, otherCluster2.Cluster})

		foundEksCluster, _ := blesser.awsConn.GetEKSCluster(ctx, testRegion, wantCluster.Crt)
		assert.NotNil(t, foundEksCluster)
	})

	t.Run("k8sConn", func(t *testing.T) {
		var (
			foundUsername string
			k8sConn       *k8sConn
			err           error
		)
		fakeSessionWrapper := &FakeAWSSessionWrapper{session: &FakeAWSSession{}}
		testRole := "test-role"
		testToken := "test-token"
		testRegion := "us-west-2"
		testUsername := "system:serviceaccount:default:someService"
		cluster := newClusterHelper("test-cluster", acctNum, "fake-crt", testRole, testRegion, emptyTags)

		fakeTokenReviews := &FakeTokenReviews{}
		fakeTokenReviews.TokenReviewReturn = &auth.TokenReview{
			Status: auth.TokenReviewStatus{
				User: auth.UserInfo{
					Username: testUsername,
				},
				Authenticated: true,
			},
		}
		fakeContext := &FakeContext{}
		fakeAuthV1Client := &FakeAuthV1Client{}
		fakeAuthV1Client.TokenReviewsReturn = fakeTokenReviews

		fakeSessionWrapper.GetAuthV1ClientReturn = fakeAuthV1Client
		k8sConn = newK8sConn(fakeSessionWrapper, cluster.Cluster, testRegion, cluster.Crt, testToken)

		foundUsername, err = k8sConn.GetK8sUsername(fakeContext)
		assert.NoError(t, err)
		assert.NotNil(t, foundUsername)
		assert.Equal(t, testUsername, foundUsername)

		// test failure outputs
		fakeTokenReviews.TokenReviewReturn = &auth.TokenReview{
			Status: auth.TokenReviewStatus{
				User: auth.UserInfo{
					Username: "",
				},
				Authenticated: false,
			},
		}
		k8sConn = newK8sConn(fakeSessionWrapper, cluster.Cluster, testRegion, cluster.Crt, testToken)
		foundUsername, err = k8sConn.GetK8sUsername(fakeContext)
		assert.NotNil(t, err)
		assert.Empty(t, foundUsername)
		assert.Equal(t, err, errors.New("requestToken authentication failed"))
	})

	t.Run("CreateK8sExtension", func(t *testing.T) {
		var (
			err       error
			cluster   *ClusterHelper
			extension string
		)
		testRole := "test-role"
		testRegion := "us-west-2"
		testNamespace := "default"
		clusterName := "test-cluster"
		serviceAccountName := "someService"
		testUsername := "system:serviceaccount:" + testNamespace + ":" + serviceAccountName
		fakeContext := &FakeContext{}

		// test default cluster naming
		cluster = newClusterHelper(clusterName, acctNum, "fake-crt", testRole, testRegion, emptyTags)
		extension, err = CreateK8sExtension(fakeContext, cluster.Cluster, testUsername, testNamespace)
		assert.NoError(t, err)
		assert.Equal(t, "k8s:"+acctNum+":test-cluster:someService", extension)

		// test cluster a/b
		tags := make(map[string]*string)
		clusterMode := "A"
		tags["ClusterName"] = &clusterName
		tags["ClusterMode"] = &clusterMode
		cluster = newClusterHelper(clusterName+"-a", acctNum, "fake-crt", testRole, testRegion, tags)
		extension, err = CreateK8sExtension(fakeContext, cluster.Cluster, testUsername, testNamespace)
		assert.Nil(t, err)
		assert.Equal(t, "k8s:"+acctNum+":"+clusterName+":"+serviceAccountName, extension)
	})

	t.Run("BlessK8s", func(t *testing.T) {
		testRole := "test-role"
		testToken := "test-token"
		testRegion := "us-west-2"
		testNamespace := "default"
		clusterName := "test-cluster"
		serviceAccountName := "someService"
		testUsername := "system:serviceaccount:" + testNamespace + ":" + serviceAccountName
		accountIDs := []string{acctNum}
		awsRegions := []string{testRegion}

		// tags for the ab Cluster
		tags := make(map[string]*string)
		clusterMode := "A"
		tags["ClusterName"] = &clusterName
		tags["ClusterMode"] = &clusterMode

		// setup fake clusters, lg = legacy, ab = with cluster a/b
		lgCluster := newClusterHelper(clusterName, acctNum, "lg-crt", testRole, testRegion, emptyTags)
		abCluster := newClusterHelper(clusterName+"-a", acctNum, "ab-crt", testRole, testRegion, tags)

		// creating clusters list
		clusters := []string{lgCluster.Name, abCluster.Name}

		// outputs list for the desired client output
		var clusterOutputs = make(map[string]*eks.DescribeClusterOutput)
		clusterOutputs[lgCluster.Name] = lgCluster.ClusterOutput
		clusterOutputs[abCluster.Name] = abCluster.ClusterOutput

		// assigning pointer to cluster names to cluster ptrs list
		clusterPtrs := []*string{}
		for i := range clusters {
			clusterPtrs = append(clusterPtrs, &clusters[i])
		}
		// setup fake token reviews
		fakeTokenReviews := &FakeTokenReviews{}
		fakeTokenReviews.TokenReviewReturn = &auth.TokenReview{
			Status: auth.TokenReviewStatus{
				User: auth.UserInfo{
					Username: testUsername,
				},
				Authenticated: true,
			},
		}

		// setup fake authv1 client
		fakeAuthV1Client := &FakeAuthV1Client{}
		fakeAuthV1Client.TokenReviewsReturn = fakeTokenReviews

		// setup fake session wrapper
		fakeSessionWrapper := &FakeAWSSessionWrapper{session: &FakeAWSSession{}}
		fakeSessionWrapper.ListEKSClustersReturn = &eks.ListClustersOutput{
			Clusters: clusterPtrs,
		}
		fakeSessionWrapper.AllEKSClusters = clusterOutputs
		fakeSessionWrapper.GetAuthV1ClientReturn = fakeAuthV1Client

		assert.NoError(t, ref.EnvClearCredentials())

		// setup fake blessings server
		pathEnv := "PATH=" + os.Getenv("PATH")
		exe := testutil.GoExecutable(t, "//go/src/github.com/grailbio/base/cmd/grail-access/grail-access")

		var blesserEndpoint naming.Endpoint
		ctx, blesserEndpoint = ticketServerUtil.RunBlesserServer(
			ctx,
			t,
			identity.K8sBlesserServer(newK8sBlesser(fakeSessionWrapper, time.Hour, testRole, accountIDs, awsRegions)),
		)

		var (
			tmpDir           string
			cleanUp          func()
			stdout           string
			principalDir     string
			principalCleanUp func()
			cmd              *exec.Cmd
		)

		// create local crt, namespace, and tokens for the legacy cluster
		tmpDir, cleanUp = testutil.TempDir(t, "", "")
		defer cleanUp()

		assert.NoError(t, ioutil.WriteFile(path.Join(tmpDir, "caCrt"), []byte(lgCluster.Crt), 0644))
		assert.NoError(t, ioutil.WriteFile(path.Join(tmpDir, "namespace"), []byte(testNamespace), 0644))
		assert.NoError(t, ioutil.WriteFile(path.Join(tmpDir, "token"), []byte(testToken), 0644))

		// Run grail-access to create a principal and bless it with the k8s flow.
		principalDir, principalCleanUp = testutil.TempDir(t, "", "")
		defer principalCleanUp()
		cmd = exec.Command(exe,
			"-dir", principalDir,
			"-blesser", fmt.Sprintf("/%s", blesserEndpoint.Address),
			"-k8s",
			"-ca-crt", path.Join(tmpDir, "caCrt"),
			"-namespace", path.Join(tmpDir, "namespace"),
			"-token", path.Join(tmpDir, "token"),
		)
		cmd.Env = []string{pathEnv}
		stdout, _ = ticketServerUtil.RunAndCapture(t, cmd)
		assert.Contains(t, stdout, "k8s:111111111111:test-cluster:someService")

		// create local crt, namespace, and tokens for the a/b cluster
		tmpDir, cleanUp = testutil.TempDir(t, "", "")
		defer cleanUp()

		assert.NoError(t, ioutil.WriteFile(path.Join(tmpDir, "caCrt"), []byte(abCluster.Crt), 0644))
		assert.NoError(t, ioutil.WriteFile(path.Join(tmpDir, "namespace"), []byte(testNamespace), 0644))
		assert.NoError(t, ioutil.WriteFile(path.Join(tmpDir, "token"), []byte(testToken), 0644))

		// Run grail-access to create a principal and bless it with the k8s flow.
		principalDir, principalCleanUp = testutil.TempDir(t, "", "")
		defer principalCleanUp()
		cmd = exec.Command(exe,
			"-dir", principalDir,
			"-blesser", fmt.Sprintf("/%s", blesserEndpoint.Address),
			"-k8s",
			"-ca-crt", path.Join(tmpDir, "caCrt"),
			"-namespace", path.Join(tmpDir, "namespace"),
			"-token", path.Join(tmpDir, "token"),
		)
		cmd.Env = []string{pathEnv}
		stdout, _ = ticketServerUtil.RunAndCapture(t, cmd)
		assert.Contains(t, stdout, "k8s:111111111111:test-cluster:someService")
	})
}
