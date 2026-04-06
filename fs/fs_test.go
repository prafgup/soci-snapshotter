/*
   Copyright The Soci Snapshotter Authors.

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

/*
   Copyright The containerd Authors.

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

/*
   Copyright 2019 The Go Authors. All rights reserved.
   Use of this source code is governed by a BSD-style
   license that can be found in the NOTICE.md file.
*/

package fs

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	ctdsnapshotters "github.com/containerd/containerd/pkg/snapshotters"

	"github.com/awslabs/soci-snapshotter/config"
	"github.com/awslabs/soci-snapshotter/fs/layer"
	"github.com/awslabs/soci-snapshotter/fs/remote"
	"github.com/awslabs/soci-snapshotter/fs/source"
	"github.com/awslabs/soci-snapshotter/idtools"
	"github.com/awslabs/soci-snapshotter/soci/store"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes/docker"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// failingTransport is an http.RoundTripper that always returns an error.
type failingTransport struct{}

func (t *failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("simulated auth failure")
}

func failingClient() *http.Client {
	return &http.Client{Transport: &failingTransport{}}
}

func TestCheck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bl := &breakableLayer{}
	fs := &filesystem{
		layer: map[string]layer.Layer{
			"test": bl,
		},
		getSources: source.FromDefaultLabels(func(imgRefSpec reference.Spec) (hosts []docker.RegistryHost, _ error) {
			return docker.ConfigureDefaultRegistries(docker.WithPlainHTTP(docker.MatchLocalhost))(imgRefSpec.Hostname())
		}),
	}
	bl.success = true
	if err := fs.Check(ctx, "test", nil); err != nil {
		t.Errorf("connection failed; wanted to succeed: %v", err)
	}

	bl.success = false
	if err := fs.Check(ctx, "test", nil); err == nil {
		t.Errorf("connection succeeded; wanted to fail")
	}
}

func TestCheckSucceedsAfterInvalidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var invalidatedRef string
	invalidated := false

	bl := &breakableLayer{}
	bl.success = false

	expectedRef := reference.Spec{Locator: "docker.io/library/alpine", Object: "latest"}

	fs := &filesystem{
		layer: map[string]layer.Layer{
			"test": bl,
		},
		getSources: func(labels map[string]string) ([]source.Source, error) {
			// After invalidation, let Refresh succeed.
			if invalidated {
				bl.success = true
			}
			return []source.Source{
				{Name: expectedRef},
			}, nil
		},
		invalidateHosts: func(ref string) {
			invalidatedRef = ref
			invalidated = true
		},
	}

	if err := fs.Check(ctx, "test", nil); err != nil {
		t.Errorf("connection failed after invalidation; wanted to succeed: %v", err)
	}
	if invalidatedRef != expectedRef.String() {
		t.Errorf("invalidateHosts called with %q; wanted %q", invalidatedRef, expectedRef.String())
	}
}

func TestCheckFailsWithoutInvalidateHosts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bl := &breakableLayer{}
	bl.success = false

	fs := &filesystem{
		layer: map[string]layer.Layer{
			"test": bl,
		},
		getSources: func(labels map[string]string) ([]source.Source, error) {
			return []source.Source{
				{Name: reference.Spec{Locator: "docker.io/library/alpine", Object: "latest"}},
			}, nil
		},
		invalidateHosts: nil,
	}

	if err := fs.Check(ctx, "test", nil); err == nil {
		t.Errorf("connection succeeded; wanted to fail")
	}
}

func TestMountRetriesWithInvalidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	invalidated := false
	getSourcesCount := 0

	imgDigest := "sha256:abc123"
	ref := reference.Spec{Locator: "docker.io/library/alpine", Object: "latest"}
	labels := map[string]string{
		ctdsnapshotters.TargetRefLabel:            "docker.io/library/alpine:latest",
		ctdsnapshotters.TargetManifestDigestLabel: imgDigest,
	}

	fs := &filesystem{
		ctx:          ctx,
		mountTimeout: 1 * time.Second,
		getSources: func(labels map[string]string) ([]source.Source, error) {
			getSourcesCount++
			return []source.Source{{
				Name:  ref,
				Hosts: []docker.RegistryHost{{Client: failingClient()}},
			}}, nil
		},
		invalidateHosts: func(ref string) {
			invalidated = true
		},
	}

	// Pre-poison the sociContext cache with a cached error.
	sc := &sociContext{}
	sc.fetchOnce.Do(func() { sc.cachedErr = fmt.Errorf("simulated stale credentials") })
	fs.sociContexts.Store(imgDigest, sc)

	// Mount will fail (both attempts), but we verify the retry mechanism.
	err := fs.Mount(ctx, "/tmp/test-mount", labels)
	if err == nil {
		t.Fatal("expected Mount to fail")
	}
	if !invalidated {
		t.Error("invalidateHosts was not called")
	}
	if _, loaded := fs.sociContexts.Load(imgDigest); !loaded {
		t.Error("expected sociContext to be re-created after invalidation")
	}
	if getSourcesCount < 2 {
		t.Errorf("expected getSources to be called at least twice, got %d", getSourcesCount)
	}
}

func TestMountLocalRetriesWithInvalidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	invalidated := false
	getSourcesCount := 0

	ref := reference.Spec{Locator: "docker.io/library/alpine", Object: "latest"}
	labels := map[string]string{
		ctdsnapshotters.TargetRefLabel: "docker.io/library/alpine:latest",
	}

	fs := &filesystem{
		getSources: func(labels map[string]string) ([]source.Source, error) {
			getSourcesCount++
			return []source.Source{{
				Name:  ref,
				Hosts: []docker.RegistryHost{{Client: failingClient()}},
				Target: ocispec.Descriptor{
					Digest: digest.FromString("test"),
				},
			}}, nil
		},
		invalidateHosts: func(ref string) {
			invalidated = true
		},
	}

	// MountLocal will fail (no real registry), but we verify retry happened.
	err := fs.MountLocal(ctx, "/tmp/test-mount-local", labels, nil)
	if err == nil {
		t.Fatal("expected MountLocal to fail")
	}
	if !invalidated {
		t.Error("invalidateHosts was not called on MountLocal failure")
	}
	if getSourcesCount < 2 {
		t.Errorf("expected getSources to be called at least twice, got %d", getSourcesCount)
	}
}

func TestMountParallelRetriesWithInvalidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	invalidated := false
	getSourcesCount := 0

	ref := reference.Spec{Locator: "docker.io/library/alpine", Object: "latest"}
	imgDigest := "sha256:abc123"
	labels := map[string]string{
		ctdsnapshotters.TargetRefLabel:            "docker.io/library/alpine:latest",
		ctdsnapshotters.TargetManifestDigestLabel: imgDigest,
	}

	fs := &filesystem{
		pullModes: config.PullModes{Parallel: config.Parallel{Enable: true}},
		containerd: store.NewContainerdClient(""), // empty address → getImageManifest fails
		inProgressImageUnpacks: &unpackJobs{
			images: make(map[string]*imageUnpackJob),
			mu:     sync.Mutex{},
		},
		getSources: func(labels map[string]string) ([]source.Source, error) {
			getSourcesCount++
			return []source.Source{{
				Name:  ref,
				Hosts: []docker.RegistryHost{{Client: failingClient()}},
				Target: ocispec.Descriptor{
					Digest: digest.FromString("test"),
				},
			}}, nil
		},
		invalidateHosts: func(ref string) {
			invalidated = true
		},
	}

	err := fs.MountParallel(ctx, "/tmp/test-mount-parallel", labels, nil)
	if err == nil {
		t.Fatal("expected MountParallel to fail")
	}
	if !invalidated {
		t.Error("invalidateHosts was not called on MountParallel failure")
	}
	if getSourcesCount < 2 {
		t.Errorf("expected getSources to be called at least twice, got %d", getSourcesCount)
	}
}

func TestMountLocalNoRetryWithoutInvalidateHosts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	getSourcesCount := 0

	ref := reference.Spec{Locator: "docker.io/library/alpine", Object: "latest"}
	labels := map[string]string{
		ctdsnapshotters.TargetRefLabel: "docker.io/library/alpine:latest",
	}

	fs := &filesystem{
		getSources: func(labels map[string]string) ([]source.Source, error) {
			getSourcesCount++
			return []source.Source{{
				Name:  ref,
				Hosts: []docker.RegistryHost{{Client: failingClient()}},
				Target: ocispec.Descriptor{
					Digest: digest.FromString("test"),
				},
			}}, nil
		},
		invalidateHosts: nil,
	}

	err := fs.MountLocal(ctx, "/tmp/test-mount-local", labels, nil)
	if err == nil {
		t.Fatal("expected MountLocal to fail")
	}
	if getSourcesCount != 1 {
		t.Errorf("expected getSources called once (no retry), got %d", getSourcesCount)
	}
}

type breakableLayer struct {
	success bool
}

func (l *breakableLayer) Info() layer.Info {
	return layer.Info{
		Size: 1,
	}
}
func (l *breakableLayer) DisableXAttrs() bool { return false }
func (l *breakableLayer) RootNode(uint32, idtools.IDMap) (fusefs.InodeEmbedder, error) {
	return nil, nil
}
func (l *breakableLayer) Verify(tocDigest digest.Digest) error { return nil }
func (l *breakableLayer) SkipVerify()                          {}
func (l *breakableLayer) ReadAt([]byte, int64, ...remote.Option) (int, error) {
	return 0, fmt.Errorf("fail")
}
func (l *breakableLayer) GetCacheRefKey() string { return "" }
func (l *breakableLayer) BackgroundFetch() error { return fmt.Errorf("fail") }
func (l *breakableLayer) Check() error {
	if !l.success {
		return fmt.Errorf("failed")
	}
	return nil
}
func (l *breakableLayer) Refresh(ctx context.Context, hosts []docker.RegistryHost, refspec reference.Spec, desc ocispec.Descriptor) error {
	if !l.success {
		return fmt.Errorf("failed")
	}
	return nil
}
func (l *breakableLayer) Done() {}
