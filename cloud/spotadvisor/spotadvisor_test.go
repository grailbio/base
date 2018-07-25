// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package spotadvisor_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"

	sa "github.com/grailbio/base/cloud/spotadvisor"
)

// Contains an abridged version of a real response to make got/want comparisons easier.
const testDataPath = "./testdata/test-spot-advisor-data.json"

// TestGetAndFilterByInterruptRate tests both GetInstancesWithMaxInterruptProbability and FilterByMaxInterruptProbability.
func TestGetAndFilterByInterruptRate(t *testing.T) {
	defer setupMockTestServer(t).Close()
	adv, err := sa.NewSpotAdvisor(testLogger, context.Background().Done())
	if err != nil {
		t.Fatalf(err.Error())
	}
	tests := []struct {
		name             string
		osType           sa.OsType
		region           sa.AwsRegion
		maxInterruptProb sa.InterruptProbability
		candidates       []string
		want             []string
		wantErr          error
	}{
		{
			name:             "simple",
			osType:           sa.Windows,
			region:           sa.AwsRegion("eu-west-2"),
			candidates:       testAvailableInstanceTypes,
			maxInterruptProb: sa.LessThanFivePct,
			want:             []string{"r4.xlarge"},
		},
		{
			name:             "<5%",
			osType:           sa.Linux,
			region:           sa.AwsRegion("eu-west-2"),
			candidates:       testAvailableInstanceTypes,
			maxInterruptProb: sa.LessThanFivePct,
			want:             []string{"m5a.4xlarge"},
		},
		{
			name:             "<10%",
			osType:           sa.Linux,
			region:           sa.AwsRegion("eu-west-2"),
			candidates:       testAvailableInstanceTypes,
			maxInterruptProb: sa.LessThanTenPct,
			want:             []string{"m5a.4xlarge", "t3.nano"},
		},
		{
			name:             "<15%",
			osType:           sa.Linux,
			region:           sa.AwsRegion("eu-west-2"),
			candidates:       testAvailableInstanceTypes,
			maxInterruptProb: sa.LessThanFifteenPct,
			want:             []string{"m5a.4xlarge", "t3.nano", "g4dn.12xlarge"},
		},
		{
			name:             "<20%",
			osType:           sa.Linux,
			region:           sa.AwsRegion("eu-west-2"),
			candidates:       testAvailableInstanceTypes,
			maxInterruptProb: sa.LessThanTwentyPct,
			want:             []string{"m5a.4xlarge", "t3.nano", "g4dn.12xlarge", "r5d.8xlarge"},
		},
		{
			name:             "Any",
			osType:           sa.Linux,
			region:           sa.AwsRegion("eu-west-2"),
			candidates:       testAvailableInstanceTypes,
			maxInterruptProb: sa.Any,
			want:             testAvailableInstanceTypes,
		},
		{
			name:             "bad_interrupt_prob_neg",
			osType:           sa.Linux,
			region:           sa.AwsRegion("eu-west-2"),
			candidates:       testAvailableInstanceTypes,
			maxInterruptProb: sa.InterruptProbability(-1),
			want:             nil,
			wantErr:          fmt.Errorf("invalid InterruptProbability: -1"),
		},
		{
			name:             "bad_interrupt_prob_pos",
			osType:           sa.Linux,
			region:           sa.AwsRegion("eu-west-2"),
			candidates:       testAvailableInstanceTypes,
			maxInterruptProb: sa.InterruptProbability(6),
			want:             nil,
			wantErr:          fmt.Errorf("invalid InterruptProbability: 6"),
		},
		{
			name:             "bad_instance_region",
			osType:           sa.Linux,
			region:           sa.AwsRegion("us-foo-2"),
			candidates:       testAvailableInstanceTypes,
			maxInterruptProb: sa.LessThanFifteenPct,
			want:             nil,
			wantErr:          fmt.Errorf("no spot advisor data for: {Linux, us-foo-2, < 15%%}"),
		},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("%s_%s_%s_%d", tt.name, tt.osType, tt.region, tt.maxInterruptProb)
		t.Run(name, func(t *testing.T) {
			got, gotErr := adv.FilterByMaxInterruptProbability(tt.osType, tt.region, tt.candidates, tt.maxInterruptProb)
			checkErr(t, tt.wantErr, gotErr)
			if tt.wantErr == nil {
				checkEqual(t, tt.want, got)
			}
		})
	}
}

func TestGetInterruptRange(t *testing.T) {
	defer setupMockTestServer(t).Close()
	adv, err := sa.NewSpotAdvisor(testLogger, context.Background().Done())
	if err != nil {
		t.Fatalf(err.Error())
	}
	tests := []struct {
		name         string
		osType       sa.OsType
		region       sa.AwsRegion
		instanceType sa.InstanceType
		want         sa.InterruptRange
		wantErr      error
	}{
		{
			name:         "simple",
			osType:       sa.Windows,
			region:       sa.AwsRegion("us-west-2"),
			instanceType: "c5a.24xlarge",
			want:         sa.TenToFifteenPct,
		},
		{
			name:         "bad_region",
			osType:       sa.Windows,
			region:       sa.AwsRegion("us-foo-2"),
			instanceType: "c5a.24xlarge",
			want:         -1,
			wantErr:      fmt.Errorf("no spot advisor data for: us-foo-2"),
		},
		{
			name:         "bad_os",
			osType:       sa.OsType("Unix"),
			region:       sa.AwsRegion("us-west-2"),
			instanceType: "c5a.24xlarge",
			want:         -1,
			wantErr:      fmt.Errorf("invalid OS: Unix"),
		},
		{
			name:         "bad_instance_type",
			osType:       sa.Linux,
			region:       sa.AwsRegion("us-west-2"),
			instanceType: "foo.bar",
			want:         -1,
			wantErr:      fmt.Errorf("no spot advisor data for Linux instance type 'foo.bar' in us-west-2"),
		},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("%s_%s_%s", tt.name, tt.osType, tt.region)
		t.Run(name, func(t *testing.T) {
			got, gotErr := adv.GetInterruptRange(tt.osType, tt.region, tt.instanceType)
			checkErr(t, tt.wantErr, gotErr)
			if tt.wantErr == nil && tt.want != got {
				t.Fatalf("want: %s, got: %s", tt.want, got)
			}
		})
	}
}

func TestGetMaxInterruptProbability(t *testing.T) {
	defer setupMockTestServer(t).Close()
	adv, err := sa.NewSpotAdvisor(testLogger, context.Background().Done())
	if err != nil {
		t.Fatalf(err.Error())
	}
	tests := []struct {
		name         string
		osType       sa.OsType
		region       sa.AwsRegion
		instanceType sa.InstanceType
		want         sa.InterruptProbability
		wantErr      error
	}{
		{
			name:         "simple_<5%",
			osType:       sa.Linux,
			region:       sa.AwsRegion("eu-west-2"),
			instanceType: "m5a.4xlarge",
			want:         sa.LessThanFivePct,
		},
		{
			name:         "simple_<10%",
			osType:       sa.Linux,
			region:       sa.AwsRegion("eu-west-2"),
			instanceType: "t3.nano",
			want:         sa.LessThanTenPct,
		},
		{
			name:         "simple_<15%",
			osType:       sa.Linux,
			region:       sa.AwsRegion("eu-west-2"),
			instanceType: "g4dn.12xlarge",
			want:         sa.LessThanFifteenPct,
		},
		{
			name:         "simple_<20%",
			osType:       sa.Linux,
			region:       sa.AwsRegion("eu-west-2"),
			instanceType: "r5d.8xlarge",
			want:         sa.LessThanTwentyPct,
		},
		{
			name:         "simple_Any",
			osType:       sa.Linux,
			region:       sa.AwsRegion("eu-west-2"),
			instanceType: "i3.2xlarge",
			want:         sa.Any,
		},
		{
			name:         "bad_region",
			osType:       sa.Windows,
			region:       sa.AwsRegion("us-foo-2"),
			instanceType: "c5a.24xlarge",
			want:         -1,
			wantErr:      fmt.Errorf("no spot advisor data for: us-foo-2"),
		},
		{
			name:         "bad_os",
			osType:       sa.OsType("Unix"),
			region:       sa.AwsRegion("us-west-2"),
			instanceType: "c5a.24xlarge",
			want:         -1,
			wantErr:      fmt.Errorf("invalid OS: Unix"),
		},
		{
			name:         "bad_instance_type",
			osType:       sa.Linux,
			region:       sa.AwsRegion("us-west-2"),
			instanceType: "foo.bar",
			want:         -1,
			wantErr:      fmt.Errorf("no spot advisor data for Linux instance type 'foo.bar' in us-west-2"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotErr := adv.GetMaxInterruptProbability(tt.osType, tt.region, tt.instanceType)
			checkErr(t, tt.wantErr, gotErr)
			if tt.wantErr == nil && tt.want != got {
				t.Fatalf("want: %s, got: %s", tt.want, got)
			}
		})
	}

}

// setupMockTestServer starts a test server and replaces the actual spot advisor
// data URL with the test server's URL. A request to the server will return the
// contents of the file at testDataPath. The caller is expected to call Close()
// on the returned test server.
func setupMockTestServer(t *testing.T) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadFile(testDataPath)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(b); err != nil {
			t.Fatal(err)
		}
	}))
	sa.SetSpotAdvisorDataUrl(ts.URL)
	return ts
}

func checkEqual(t *testing.T, want []string, got []string) {
	if len(want) != len(got) {
		t.Fatalf("\nwant:\t%s\ngot:\t%s", want, got)
	}
	sort.Strings(want)
	sort.Strings(got)
	if !reflect.DeepEqual(got, got) {
		t.Fatalf("\nwant:\t%s\ngot:\t%s", want, got)
	}
}

func checkErr(t *testing.T, want error, got error) {
	if want != nil && got != nil {
		if want.Error() != got.Error() {
			t.Fatalf("want: %s, got: %s", want, got)
		} else {
			return
		}
	}
	if want != got {
		t.Fatalf("want: %s, got: %s", want, got)
	}
}

var testLogger = log.New(ioutil.Discard, "", 0)

var testAvailableInstanceTypes = []string{
	"a1.2xlarge", "a1.4xlarge", "a1.large", "a1.metal", "a1.xlarge", "c1.xlarge", "c3.2xlarge", "c3.4xlarge", "c3.8xlarge", "c3.large", "c3.xlarge",
	"c4.2xlarge", "c4.4xlarge", "c4.8xlarge", "c4.large", "c4.xlarge", "c5.12xlarge", "c5.18xlarge", "c5.24xlarge", "c5.2xlarge", "c5.4xlarge", "c5.9xlarge",
	"c5.large", "c5.metal", "c5.xlarge", "c5a.12xlarge", "c5a.16xlarge", "c5a.24xlarge", "c5a.2xlarge", "c5a.4xlarge", "c5a.8xlarge", "c5a.large", "c5a.xlarge",
	"c5ad.12xlarge", "c5ad.16xlarge", "c5ad.24xlarge", "c5ad.2xlarge", "c5ad.4xlarge", "c5ad.8xlarge", "c5ad.large", "c5ad.xlarge", "c5d.12xlarge", "c5d.18xlarge",
	"c5d.24xlarge", "c5d.2xlarge", "c5d.4xlarge", "c5d.9xlarge", "c5d.large", "c5d.metal", "c5d.xlarge", "c5n.18xlarge", "c5n.2xlarge", "c5n.4xlarge", "c5n.9xlarge",
	"c5n.large", "c5n.metal", "c5n.xlarge", "c6g.12xlarge", "c6g.16xlarge", "c6g.2xlarge", "c6g.4xlarge", "c6g.8xlarge", "c6g.large", "c6g.metal", "c6g.xlarge",
	"c6gd.12xlarge", "c6gd.16xlarge", "c6gd.2xlarge", "c6gd.4xlarge", "c6gd.8xlarge", "c6gd.large", "c6gd.metal", "c6gd.xlarge", "c6gn.12xlarge", "c6gn.16xlarge", "c6gn.2xlarge",
	"c6gn.4xlarge", "c6gn.8xlarge", "c6gn.large", "c6gn.xlarge", "cr1.8xlarge", "d2.2xlarge", "d2.4xlarge", "d2.8xlarge", "d2.xlarge", "d3.2xlarge", "d3.4xlarge",
	"d3.8xlarge", "d3.xlarge", "d3en.12xlarge", "d3en.2xlarge", "d3en.4xlarge", "d3en.6xlarge", "d3en.8xlarge", "d3en.xlarge", "f1.16xlarge", "f1.2xlarge", "f1.4xlarge",
	"g2.2xlarge", "g2.8xlarge", "g3.16xlarge", "g3.4xlarge", "g3.8xlarge", "g3s.xlarge", "g4ad.16xlarge", "g4ad.4xlarge", "g4ad.8xlarge", "g4dn.12xlarge", "g4dn.16xlarge",
	"g4dn.2xlarge", "g4dn.4xlarge", "g4dn.8xlarge", "g4dn.metal", "g4dn.xlarge", "h1.16xlarge", "h1.2xlarge", "h1.4xlarge", "h1.8xlarge", "hs1.8xlarge", "i2.2xlarge",
	"i2.4xlarge", "i2.8xlarge", "i2.xlarge", "i3.16xlarge", "i3.2xlarge", "i3.4xlarge", "i3.8xlarge", "i3.large", "i3.metal", "i3.xlarge", "i3en.12xlarge",
	"i3en.24xlarge", "i3en.2xlarge", "i3en.3xlarge", "i3en.6xlarge", "i3en.large", "i3en.metal", "i3en.xlarge", "inf1.24xlarge", "inf1.2xlarge", "inf1.6xlarge", "inf1.xlarge",
	"m1.large", "m1.xlarge", "m2.2xlarge", "m2.4xlarge", "m2.xlarge", "m3.2xlarge", "m3.large", "m3.xlarge", "m4.10xlarge", "m4.16xlarge", "m4.2xlarge",
	"m4.4xlarge", "m4.large", "m4.xlarge", "m5.12xlarge", "m5.16xlarge", "m5.24xlarge", "m5.2xlarge", "m5.4xlarge", "m5.8xlarge", "m5.large", "m5.metal",
	"m5.xlarge", "m5a.12xlarge", "m5a.16xlarge", "m5a.24xlarge", "m5a.2xlarge", "m5a.4xlarge", "m5a.8xlarge", "m5a.large", "m5a.xlarge", "m5ad.12xlarge", "m5ad.16xlarge",
	"m5ad.24xlarge", "m5ad.2xlarge", "m5ad.4xlarge", "m5ad.8xlarge", "m5ad.large", "m5ad.xlarge", "m5d.12xlarge", "m5d.16xlarge", "m5d.24xlarge", "m5d.2xlarge", "m5d.4xlarge",
	"m5d.8xlarge", "m5d.large", "m5d.metal", "m5d.xlarge", "m5dn.12xlarge", "m5dn.16xlarge", "m5dn.24xlarge", "m5dn.2xlarge", "m5dn.4xlarge", "m5dn.8xlarge", "m5dn.large",
	"m5dn.metal", "m5dn.xlarge", "m5n.12xlarge", "m5n.16xlarge", "m5n.24xlarge", "m5n.2xlarge", "m5n.4xlarge", "m5n.8xlarge", "m5n.large", "m5n.metal", "m5n.xlarge",
	"m5zn.12xlarge", "m5zn.2xlarge", "m5zn.3xlarge", "m5zn.6xlarge", "m5zn.large", "m5zn.metal", "m5zn.xlarge", "m6g.12xlarge", "m6g.16xlarge", "m6g.2xlarge", "m6g.4xlarge",
	"m6g.8xlarge", "m6g.large", "m6g.metal", "m6g.xlarge", "m6gd.12xlarge", "m6gd.16xlarge", "m6gd.2xlarge", "m6gd.4xlarge", "m6gd.8xlarge", "m6gd.large", "m6gd.metal",
	"m6gd.xlarge", "p2.16xlarge", "p2.8xlarge", "p2.xlarge", "p3.16xlarge", "p3.2xlarge", "p3.8xlarge", "p3dn.24xlarge", "p4d.24xlarge", "r3.2xlarge", "r3.4xlarge",
	"r3.8xlarge", "r3.large", "r3.xlarge", "r4.16xlarge", "r4.2xlarge", "r4.4xlarge", "r4.8xlarge", "r4.large", "r4.xlarge", "r5.12xlarge", "r5.16xlarge",
	"r5.24xlarge", "r5.2xlarge", "r5.4xlarge", "r5.8xlarge", "r5.large", "r5.metal", "r5.xlarge", "r5a.12xlarge", "r5a.16xlarge", "r5a.24xlarge", "r5a.2xlarge",
	"r5a.4xlarge", "r5a.8xlarge", "r5a.large", "r5a.xlarge", "r5ad.12xlarge", "r5ad.16xlarge", "r5ad.24xlarge", "r5ad.2xlarge", "r5ad.4xlarge", "r5ad.8xlarge", "r5ad.large",
	"r5ad.xlarge", "r5b.12xlarge", "r5b.16xlarge", "r5b.24xlarge", "r5b.2xlarge", "r5b.4xlarge", "r5b.8xlarge", "r5b.large", "r5b.metal", "r5b.xlarge", "r5d.12xlarge",
	"r5d.16xlarge", "r5d.24xlarge", "r5d.2xlarge", "r5d.4xlarge", "r5d.8xlarge", "r5d.large", "r5d.metal", "r5d.xlarge", "r5dn.12xlarge", "r5dn.16xlarge", "r5dn.24xlarge",
	"r5dn.2xlarge", "r5dn.4xlarge", "r5dn.8xlarge", "r5dn.large", "r5dn.metal", "r5dn.xlarge", "r5n.12xlarge", "r5n.16xlarge", "r5n.24xlarge", "r5n.2xlarge", "r5n.4xlarge",
	"r5n.8xlarge", "r5n.large", "r5n.metal", "r5n.xlarge", "r6g.12xlarge", "r6g.16xlarge", "r6g.2xlarge", "r6g.4xlarge", "r6g.8xlarge", "r6g.large", "r6g.metal",
	"r6g.xlarge", "r6gd.12xlarge", "r6gd.16xlarge", "r6gd.2xlarge", "r6gd.4xlarge", "r6gd.8xlarge", "r6gd.large", "r6gd.metal", "r6gd.xlarge", "t1.micro", "t2.2xlarge",
	"t2.large", "t2.micro", "t2.nano", "t2.xlarge", "t3.2xlarge", "t3.large", "t3.micro", "t3.nano", "t3.xlarge", "t3a.2xlarge", "t3a.large",
	"t3a.micro", "t3a.nano", "t3a.xlarge", "t4g.2xlarge", "t4g.large", "t4g.micro", "t4g.nano", "t4g.xlarge", "u-12tb1.112xlarge", "u-6tb1.112xlarge", "u-6tb1.56xlarge",
	"u-9tb1.112xlarge", "x1.16xlarge", "x1.32xlarge", "x1e.16xlarge", "x1e.2xlarge", "x1e.32xlarge", "x1e.4xlarge", "x1e.8xlarge", "x1e.xlarge", "x2gd.12xlarge", "x2gd.16xlarge",
	"x2gd.2xlarge", "x2gd.4xlarge", "x2gd.8xlarge", "x2gd.large", "x2gd.metal", "x2gd.xlarge", "z1d.12xlarge", "z1d.2xlarge", "z1d.3xlarge", "z1d.6xlarge", "z1d.large",
	"z1d.metal", "z1d.xlarge",
}
