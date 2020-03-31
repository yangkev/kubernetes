/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cronjob

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/robfig/cron"

	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func boolptr(b bool) *bool { return &b }

func TestGetJobFromTemplate(t *testing.T) {
	// getJobFromTemplate() needs to take the job template and copy the labels and annotations
	// and other fields, and add a created-by reference.

	var one int64 = 1
	var no bool = false

	sj := batchv1beta1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mycronjob",
			Namespace: "snazzycats",
			UID:       types.UID("1a2b3c"),
			SelfLink:  "/apis/batch/v1/namespaces/snazzycats/jobs/mycronjob",
		},
		Spec: batchv1beta1.CronJobSpec{
			Schedule:          "* * * * ?",
			ConcurrencyPolicy: batchv1beta1.AllowConcurrent,
			JobTemplate: batchv1beta1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      map[string]string{"a": "b"},
					Annotations: map[string]string{"x": "y"},
				},
				Spec: batchv1.JobSpec{
					ActiveDeadlineSeconds: &one,
					ManualSelector:        &no,
					Template: v1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								"foo": "bar",
							},
						},
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{Image: "foo/bar"},
							},
						},
					},
				},
			},
		},
	}

	var job *batchv1.Job
	job, err := getJobFromTemplate(&sj, time.Time{})
	if err != nil {
		t.Errorf("Did not expect error: %s", err)
	}
	if !strings.HasPrefix(job.ObjectMeta.Name, "mycronjob-") {
		t.Errorf("Wrong Name")
	}
	if len(job.ObjectMeta.Labels) != 1 {
		t.Errorf("Wrong number of labels")
	}
	if len(job.ObjectMeta.Annotations) != 1 {
		t.Errorf("Wrong number of annotations")
	}
}

func TestGetParentUIDFromJob(t *testing.T) {
	j := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foobar",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: batchv1.JobSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"foo": "bar"},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"foo": "bar",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Image: "foo/bar"},
					},
				},
			},
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: v1.ConditionTrue,
			}},
		},
	}
	{
		// Case 1: No ControllerRef
		_, found := getParentUIDFromJob(*j)

		if found {
			t.Errorf("Unexpectedly found uid")
		}
	}
	{
		// Case 2: Has ControllerRef
		j.ObjectMeta.SetOwnerReferences([]metav1.OwnerReference{
			{
				Kind:       "CronJob",
				UID:        types.UID("5ef034e0-1890-11e6-8935-42010af0003e"),
				Controller: boolptr(true),
			},
		})

		expectedUID := types.UID("5ef034e0-1890-11e6-8935-42010af0003e")

		uid, found := getParentUIDFromJob(*j)
		if !found {
			t.Errorf("Unexpectedly did not find uid")
		} else if uid != expectedUID {
			t.Errorf("Wrong UID: %v", uid)
		}
	}

}

func TestGroupJobsByParent(t *testing.T) {
	uid1 := types.UID("11111111-1111-1111-1111-111111111111")
	uid2 := types.UID("22222222-2222-2222-2222-222222222222")
	uid3 := types.UID("33333333-3333-3333-3333-333333333333")

	ownerReference1 := metav1.OwnerReference{
		Kind:       "CronJob",
		UID:        uid1,
		Controller: boolptr(true),
	}

	ownerReference2 := metav1.OwnerReference{
		Kind:       "CronJob",
		UID:        uid2,
		Controller: boolptr(true),
	}

	ownerReference3 := metav1.OwnerReference{
		Kind:       "CronJob",
		UID:        uid3,
		Controller: boolptr(true),
	}

	{
		// Case 1: There are no jobs and scheduledJobs
		js := []batchv1.Job{}
		jobsBySj := groupJobsByParent(js)
		if len(jobsBySj) != 0 {
			t.Errorf("Wrong number of items in map")
		}
	}

	{
		// Case 2: there is one controller with one job it created.
		js := []batchv1.Job{
			{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "x", OwnerReferences: []metav1.OwnerReference{ownerReference1}}},
		}
		jobsBySj := groupJobsByParent(js)

		if len(jobsBySj) != 1 {
			t.Errorf("Wrong number of items in map")
		}
		jobList1, found := jobsBySj[uid1]
		if !found {
			t.Errorf("Key not found")
		}
		if len(jobList1) != 1 {
			t.Errorf("Wrong number of items in map")
		}
	}

	{
		// Case 3: Two namespaces, one has two jobs from one controller, other has 3 jobs from two controllers.
		// There are also two jobs with no created-by annotation.
		js := []batchv1.Job{
			{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "x", OwnerReferences: []metav1.OwnerReference{ownerReference1}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "x", OwnerReferences: []metav1.OwnerReference{ownerReference2}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "x", OwnerReferences: []metav1.OwnerReference{ownerReference1}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "d", Namespace: "x", OwnerReferences: []metav1.OwnerReference{}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "y", OwnerReferences: []metav1.OwnerReference{ownerReference3}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "y", OwnerReferences: []metav1.OwnerReference{ownerReference3}}},
			{ObjectMeta: metav1.ObjectMeta{Name: "d", Namespace: "y", OwnerReferences: []metav1.OwnerReference{}}},
		}

		jobsBySj := groupJobsByParent(js)

		if len(jobsBySj) != 3 {
			t.Errorf("Wrong number of items in map")
		}
		jobList1, found := jobsBySj[uid1]
		if !found {
			t.Errorf("Key not found")
		}
		if len(jobList1) != 2 {
			t.Errorf("Wrong number of items in map")
		}
		jobList2, found := jobsBySj[uid2]
		if !found {
			t.Errorf("Key not found")
		}
		if len(jobList2) != 1 {
			t.Errorf("Wrong number of items in map")
		}
		jobList3, found := jobsBySj[uid3]
		if !found {
			t.Errorf("Key not found")
		}
		if len(jobList3) != 2 {
			t.Errorf("Wrong number of items in map")
		}
	}
}

func TestGetRecentUnmetScheduleTimes(t *testing.T) {
	// schedule is hourly on the hour
	schedule := "0 * * * ?"
	// T1 is a scheduled start time of that schedule
	T1, err := time.Parse(time.RFC3339, "2016-05-19T10:00:00Z")
	if err != nil {
		t.Errorf("test setup error: %v", err)
	}
	// T2 is a scheduled start time of that schedule after T1
	T2, err := time.Parse(time.RFC3339, "2016-05-19T11:00:00Z")
	if err != nil {
		t.Errorf("test setup error: %v", err)
	}

	sj := batchv1beta1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mycronjob",
			Namespace: metav1.NamespaceDefault,
			UID:       types.UID("1a2b3c"),
		},
		Spec: batchv1beta1.CronJobSpec{
			Schedule:          schedule,
			ConcurrencyPolicy: batchv1beta1.AllowConcurrent,
			JobTemplate:       batchv1beta1.JobTemplateSpec{},
		},
	}
	{
		// Case 1: no known start times, and none needed yet.
		// Creation time is before T1.
		sj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-10 * time.Minute)}
		// Current time is more than creation time, but less than T1.
		now := T1.Add(-7 * time.Minute)
		times, err := getRecentUnmetScheduleTimes(sj, now)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(times) != 0 {
			t.Errorf("expected no start times, got:  %v", times)
		}
	}
	{
		// Case 2: no known start times, and one needed.
		// Creation time is before T1.
		sj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-10 * time.Minute)}
		// Current time is after T1
		now := T1.Add(2 * time.Second)
		times, err := getRecentUnmetScheduleTimes(sj, now)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(times) != 1 {
			t.Errorf("expected 1 start time, got: %v", times)
		} else if !times[0].Equal(T1) {
			t.Errorf("expected: %v, got: %v", T1, times[0])
		}
	}
	{
		// Case 3: known LastScheduleTime, no start needed.
		// Creation time is before T1.
		sj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-10 * time.Minute)}
		// Status shows a start at the expected time.
		sj.Status.LastScheduleTime = &metav1.Time{Time: T1}
		// Current time is after T1
		now := T1.Add(2 * time.Minute)
		times, err := getRecentUnmetScheduleTimes(sj, now)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(times) != 0 {
			t.Errorf("expected 0 start times, got: %v", times)
		}
	}
	{
		// Case 4: known LastScheduleTime, a start needed
		// Creation time is before T1.
		sj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-10 * time.Minute)}
		// Status shows a start at the expected time.
		sj.Status.LastScheduleTime = &metav1.Time{Time: T1}
		// Current time is after T1 and after T2
		now := T2.Add(5 * time.Minute)
		times, err := getRecentUnmetScheduleTimes(sj, now)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(times) != 1 {
			t.Errorf("expected 1 start times, got: %v", times)
		} else if !times[0].Equal(T2) {
			t.Errorf("expected: %v, got: %v", T2, times[0])
		}
	}
	{
		// Case 5: known LastScheduleTime, two starts needed
		sj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-2 * time.Hour)}
		sj.Status.LastScheduleTime = &metav1.Time{Time: T1.Add(-1 * time.Hour)}
		// Current time is after T1 and after T2
		now := T2.Add(5 * time.Minute)
		times, err := getRecentUnmetScheduleTimes(sj, now)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(times) != 2 {
			t.Errorf("expected 2 start times, got: %v", times)
		} else {
			if !times[len(times)-1].Equal(T2) {
				t.Errorf("expected: %v, got: %v", T2, times[len(times)-1])
			}
		}
	}
	{
		// Case 6: now is way way ahead of last start time, and there is no deadline.
		sj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-2 * time.Hour)}
		sj.Status.LastScheduleTime = &metav1.Time{Time: T1.Add(-1 * time.Hour)}
		now := T2.Add(10 * 24 * time.Hour)
		times, err := getRecentUnmetScheduleTimes(sj, now)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(times) != 2 {
			t.Errorf("expected 2 start times, got: %v", times)
		} else {
			if !times[len(times)-1].Equal(now) {
				t.Errorf("expected: %v, got: %v", now, times[len(times)-1])
			}
		}
	}
	{
		// Case 7: now is way way ahead of last start time, but there is a short deadline.
		sj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-2 * time.Hour)}
		sj.Status.LastScheduleTime = &metav1.Time{Time: T1.Add(-1 * time.Hour)}
		now := T2.Add(10 * 24 * time.Hour)
		// Deadline is short
		deadline := int64(2 * 60 * 60)
		sj.Spec.StartingDeadlineSeconds = &deadline
		_, err := getRecentUnmetScheduleTimes(sj, now)
		if err != nil {
			t.Errorf("unexpected error")
		}
	}
	{
		// Case 8: now is just after deadline has passed, deadline expired with no jobs invoked
		sj.ObjectMeta.CreationTimestamp = metav1.Time{Time: T1.Add(-5 * time.Minute)}
		sj.Status.LastScheduleTime = &metav1.Time{Time: T1}
		// Deadline is 30 seconds
		deadline := int64(30)
		// Now is 10 seconds past the deadline, T2 was an unmet schedule
		now := T2.Add(time.Duration(deadline+10) * time.Second)
		sj.Spec.StartingDeadlineSeconds = &deadline
		times, err := getRecentUnmetScheduleTimes(sj, now)
		if err != nil {
			t.Errorf("unexpected error")
		}
		if len(times) != 1 {
			t.Errorf("expected 1 start time, got %v", times)
		} else {
			if !times[len(times)-1].Equal(T2) {
				t.Errorf("expected %v, got: %v", T2, times[len(times)-1])
			}
		}
	}
}

func TestByJobStartTime(t *testing.T) {
	now := metav1.NewTime(time.Date(2018, time.January, 1, 2, 3, 4, 5, time.UTC))
	later := metav1.NewTime(time.Date(2019, time.January, 1, 2, 3, 4, 5, time.UTC))
	aNil := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "a"},
		Status:     batchv1.JobStatus{},
	}
	bNil := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "b"},
		Status:     batchv1.JobStatus{},
	}
	aSet := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "a"},
		Status:     batchv1.JobStatus{StartTime: &now},
	}
	bSet := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "b"},
		Status:     batchv1.JobStatus{StartTime: &now},
	}
	aSetLater := batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "a"},
		Status:     batchv1.JobStatus{StartTime: &later},
	}

	testCases := []struct {
		name            string
		input, expected []batchv1.Job
	}{
		{
			name:     "both have nil start times",
			input:    []batchv1.Job{bNil, aNil},
			expected: []batchv1.Job{aNil, bNil},
		},
		{
			name:     "only the first has a nil start time",
			input:    []batchv1.Job{aNil, bSet},
			expected: []batchv1.Job{bSet, aNil},
		},
		{
			name:     "only the second has a nil start time",
			input:    []batchv1.Job{aSet, bNil},
			expected: []batchv1.Job{aSet, bNil},
		},
		{
			name:     "both have non-nil, equal start time",
			input:    []batchv1.Job{bSet, aSet},
			expected: []batchv1.Job{aSet, bSet},
		},
		{
			name:     "both have non-nil, different start time",
			input:    []batchv1.Job{aSetLater, bSet},
			expected: []batchv1.Job{bSet, aSetLater},
		},
	}

	for _, testCase := range testCases {
		sort.Sort(byJobStartTime(testCase.input))
		if !reflect.DeepEqual(testCase.input, testCase.expected) {
			t.Errorf("case: '%s', jobs not sorted as expected", testCase.name)
		}
	}
}

func Test_getLatestMissedSchedule(t *testing.T) {
	startTime, err := time.Parse(time.RFC3339, "2016-05-19T10:00:00Z")
	if err != nil {
		panic("test setup error")
	}

	hourly, err := cron.ParseStandard("0 * * * *")
	if err != nil {
		t.Errorf("couldn't parse schedule")
	}
	minutely, err := cron.ParseStandard("* * * * *")
	if err != nil {
		t.Errorf("couldn't parse schedule")
	}

	cases := []struct {
		desc        string
		end         time.Time
		schedule    cron.Schedule
		expectFound bool
		expect      time.Time
	}{
		{
			desc:        "basic case",
			end:         weekAfterTheHour(),
			schedule:    hourly,
			expectFound: true,
			expect:      weekAfterTheHour(),
		},
		{
			desc:        "no time",
			end:         startTime,
			schedule:    minutely,
			expectFound: false,
		},
		{
			desc:        "not enough time",
			end:         startTime.Add(time.Second * 59),
			schedule:    minutely,
			expectFound: false,
		},
		{
			desc:        "just enough time",
			end:         startTime.Add(time.Minute),
			schedule:    minutely,
			expectFound: true,
			expect:      startTime.Add(time.Minute),
		},
	}

	for _, c := range cases {
		actual, numberMissed := getLatestMissedSchedule(startTime, c.end, c.schedule)
		if (numberMissed > 0) != c.expectFound {
			t.Errorf("For case '%s' at %v, expected %v, got %v", c.schedule, c.end, c.expect, actual)
		}
		if actual != c.expect {
			t.Errorf("%v", actual)
		}
	}
}

func Test_getLatestMissedScheduleBinarySearch(t *testing.T) {
	startTime, err := time.Parse(time.RFC3339, "2016-05-19T10:00:00Z")
	if err != nil {
		panic("test setup error")
	}

	elevenOnTheHour, err := cron.ParseStandard("0 10,11 * * *")
	if err != nil {
		t.Errorf("couldn't parse schedule")
	}
	hourly, err := cron.ParseStandard("0 * * * *")
	if err != nil {
		t.Errorf("couldn't parse schedule")
	}
	minutely, err := cron.ParseStandard("* * * * *")
	if err != nil {
		t.Errorf("couldn't parse schedule")
	}

	cases := []struct {
		desc        string
		end         time.Time
		schedule    cron.Schedule
		expectFound bool
		expect      time.Time
	}{
		{
			desc:        "basic case",
			end:         weekAfterTheHour(),
			schedule:    hourly,
			expectFound: true,
			expect:      weekAfterTheHour(),
		},
		{
			desc:        "long case",
			end:         startTime.Add(time.Hour*24*500 - time.Second), // 500 days later, minus 1 second.
			schedule:    minutely,
			expectFound: true,
			expect:      startTime.Add(time.Hour*24*500 - time.Minute), // 500 days later, minus 1 minute.
		},
		{
			desc:        "no time",
			end:         startTime,
			schedule:    minutely,
			expectFound: false,
		},
		{
			desc:        "not enough time",
			end:         startTime.Add(time.Second * 59),
			schedule:    minutely,
			expectFound: false,
		},
		{
			desc:        "just enough time",
			end:         startTime.Add(time.Minute),
			schedule:    minutely,
			expectFound: true,
			expect:      startTime.Add(time.Minute),
		},
		{
			desc:        "bounds check on midway",
			end:         startTime.Add(time.Hour*2),
			schedule:    elevenOnTheHour,
			expectFound: true,
			expect:      startTime.Add(time.Hour),
		},
		{
			desc:        "result just before midway",
			end:         startTime.Add(time.Hour*2 + time.Minute), // Midway is 30s after the schedule time.
			schedule:    elevenOnTheHour,
			expectFound: true,
			expect:      startTime.Add(time.Hour),
		},
		{
			desc:        "result just after midway",
			end:         startTime.Add(time.Hour*2 - time.Minute), // Midway is 30s before the schedule time.
			schedule:    elevenOnTheHour,
			expectFound: true,
			expect:      startTime.Add(time.Hour),
		},
	}

	for _, c := range cases {
		actual, found := getLatestMissedScheduleBinarySearch(startTime, c.end, c.schedule)
		if found != c.expectFound {
			t.Errorf("For case '%s' at %v, expected %v, got %v", c.schedule, c.end, c.expect, actual)
		}
		if actual != c.expect {
			t.Errorf("%v", actual)
		}
	}
}
