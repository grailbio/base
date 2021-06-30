package spotfeed

import (
	"testing"
	"time"
)

func TestQuerier(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	iid, typ := "some-instance-id", "some-instance-type"
	entries := []*Entry{
		{ChargeUSD: 60, Timestamp: now.Add(-60 * time.Minute), InstanceID: iid, Instance: typ},
		{ChargeUSD: 110 /*ignored*/, Timestamp: now, InstanceID: iid, Instance: typ},
		{ChargeUSD: 120, Timestamp: now, InstanceID: iid, Instance: typ},
		{ChargeUSD: 80 /*ignored*/, Timestamp: now.Add(59 * time.Minute), InstanceID: iid, Instance: typ},
		{ChargeUSD: 90, Timestamp: now.Add(59 * time.Minute), InstanceID: iid, Instance: typ},
		{ChargeUSD: 120, Timestamp: now.Add(121 * time.Minute), InstanceID: iid, Instance: typ},
		{ChargeUSD: 88 /*ignored*/, Timestamp: now.Add(3 * time.Hour), InstanceID: iid, Instance: typ},
		{ChargeUSD: 89 /*ignored*/, Timestamp: now.Add(3 * time.Hour), InstanceID: iid, Instance: typ},
		{ChargeUSD: 90 /*duplicate*/, Timestamp: now.Add(3 * time.Hour), InstanceID: iid, Instance: typ},
		{ChargeUSD: 90, Timestamp: now.Add(3 * time.Hour), InstanceID: iid, Instance: typ},
	}
	terminated := now.Add(3*time.Hour + 30*time.Minute)
	q := newQuerier(entries)
	_, err := q.Query("some-other-instance-id", Period{}, time.Time{})
	if got, want := err, ErrMissingData; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	for i, tt := range []struct {
		iet   time.Time
		p     Period
		c     Cost
		wantE error
	}{
		{ // Period starting and ending before data.
			time.Time{},
			Period{now.Add(-90 * time.Minute), now.Add(-70 * time.Minute)},
			Cost{},
			ErrMissingData,
		},
		{ // Period starting and ending after data.
			time.Time{},
			Period{now.Add(3*time.Hour + 1*time.Minute), now.Add(4 * time.Hour)},
			Cost{},
			ErrMissingData,
		},
		{ // Period starting before data but ending within.
			time.Time{},
			Period{now.Add(-90 * time.Minute), now.Add(-30 * time.Minute)},
			Cost{
				Period{now.Add(-60 * time.Minute), now.Add(-30 * time.Minute)},
				60 * 30.0 / 60.0,
			},
			nil,
		},
		{ // Period starting within data and going beyond.
			terminated,
			Period{now.Add(2 * time.Hour), now.Add(4 * time.Hour)},
			Cost{
				Period{now.Add(2 * time.Hour), terminated},
				90*1.0/62.0 + 120 + 90,
			},
			nil,
		},
		{ // Period starting within data and going beyond with no terminated
			time.Time{},
			Period{now.Add(2 * time.Hour), now.Add(4 * time.Hour)},
			Cost{
				Period{now.Add(2 * time.Hour), now.Add(4 * time.Hour)},
				90*1.0/62.0 + 120 + 90,
			},
			nil,
		},
		{ // Period starting exactly at some timestamp and ending within its period.
			time.Time{},
			Period{now, now.Add(5 * time.Minute)},
			Cost{
				Period{now, now.Add(5 * time.Minute)},
				120 * 5.0 / 59.0,
			},
			nil,
		},
		{ // Period starting and within a single time period.
			time.Time{},
			Period{now.Add(1 * time.Minute), now.Add(6 * time.Minute)},
			Cost{
				Period{now.Add(1 * time.Minute), now.Add(6 * time.Minute)},
				120 * 5.0 / 59.0,
			},
			nil,
		},
		{ // Period starting exactly at some timestamp and spanning more than one.
			time.Time{},
			Period{now, now.Add(80 * time.Minute)},
			Cost{
				Period{now, now.Add(80 * time.Minute)},
				120 + 90*21.0/62.0,
			},
			nil,
		},
		{ // Period starting before data and ending after.
			terminated,
			Period{now.Add(-90 * time.Minute), now.Add(6 * time.Hour)},
			Cost{
				Period{now.Add(-60 * time.Minute), terminated},
				60 + 120 + 90 + 120 + 90,
			},
			nil,
		},
		{ // Period starting before data and ending after with no terminated.
			time.Time{},
			Period{now.Add(-90 * time.Minute), now.Add(6 * time.Hour)},
			Cost{
				Period{now.Add(-60 * time.Minute), now.Add(6 * time.Hour)},
				60 + 120 + 90 + 120 + 90,
			},
			nil,
		},
		{ // Period starting within data but ending within last period before instance end time.
			terminated,
			Period{now.Add(-90 * time.Minute), now.Add(3*time.Hour + 15*time.Minute)},
			Cost{
				Period{now.Add(-60 * time.Minute), now.Add(3*time.Hour + 15*time.Minute)},
				60 + 120 + 90 + 120 + 90*15/30.0,
			},
			nil,
		},
		{ // Period starting within data but ending within last period with no terminated.
			time.Time{},
			Period{now.Add(-90 * time.Minute), now.Add(3*time.Hour + 15*time.Minute)},
			Cost{
				Period{now.Add(-60 * time.Minute), now.Add(3*time.Hour + 15*time.Minute)},
				60 + 120 + 90 + 120 + 90,
			},
			nil,
		},
	} {
		c, err := q.Query(iid, tt.p, tt.iet)
		if tt.wantE != nil {
			if got, want := err, tt.wantE; got != want {
				t.Errorf("[%d] got %v, want %v", i, got, want)
			}
			continue
		}
		if err != nil {
			t.Error(err)
			continue
		}
		if got, want := c, tt.c; got != want {
			t.Errorf("[%d[ got %v, want %v", i, got, want)
		}
	}
}
