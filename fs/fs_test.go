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
	"testing"

	"github.com/awslabs/soci-snapshotter/fs/layer"
	"github.com/awslabs/soci-snapshotter/fs/remote"
	"github.com/awslabs/soci-snapshotter/fs/source"
	"github.com/awslabs/soci-snapshotter/idtools"
	ctdsnapshotters "github.com/containerd/containerd/pkg/snapshotters"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes/docker"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

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

	refreshCount := 0
	var invalidatedRef string

	bl := &breakableLayer{}
	bl.success = false

	expectedRef := reference.Spec{Locator: "docker.io/library/alpine", Object: "latest"}

	fs := &filesystem{
		layer: map[string]layer.Layer{
			"test": bl,
		},
		getSources: func(labels map[string]string) ([]source.Source, error) {
			refreshCount++
			// After invalidation, let Refresh succeed.
			if refreshCount == 2 {
				bl.success = true
			}
			return []source.Source{
				{Name: expectedRef},
			}, nil
		},
		invalidateHosts: func(ref string) {
			invalidatedRef = ref
		},
	}

	if err := fs.Check(ctx, "test", nil); err != nil {
		t.Errorf("connection failed after invalidation; wanted to succeed: %v", err)
	}
	if invalidatedRef != expectedRef.String() {
		t.Errorf("invalidateHosts called with %q; wanted %q", invalidatedRef, expectedRef.String())
	}
	if refreshCount != 2 {
		t.Errorf("expected 2 refreshLayer calls, got %d", refreshCount)
	}
}

func TestCheckSucceedsWithFallbackRef(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	refreshCount := 0
	var invalidatedRefs []string

	bl := &breakableLayer{}
	bl.success = false

	oldRef := reference.Spec{Locator: "registry.example.com/repo/image", Object: "old-tag"}
	newRef := reference.Spec{Locator: "registry.example.com/repo/image", Object: "new-tag"}

	fs := &filesystem{
		layer: map[string]layer.Layer{
			"test": bl,
		},
		getSources: func(labels map[string]string) ([]source.Source, error) {
			refreshCount++
			refStr := labels[ctdsnapshotters.TargetRefLabel]
			ref, _ := reference.Parse(refStr)
			// Only succeed when using the new (fallback) ref.
			if ref.String() == newRef.String() {
				bl.success = true
			}
			return []source.Source{
				{Name: ref},
			}, nil
		},
		invalidateHosts: func(ref string) {
			invalidatedRefs = append(invalidatedRefs, ref)
		},
	}

	// Set fallback ref in context (simulates what Prepare does).
	ctx = source.WithFallbackImageRef(ctx, newRef.String())

	// Labels point to the old ref (simulates stale snapshot labels).
	labels := map[string]string{
		ctdsnapshotters.TargetRefLabel: oldRef.String(),
	}

	if err := fs.Check(ctx, "test", labels); err != nil {
		t.Errorf("connection failed with fallback ref; wanted to succeed: %v", err)
	}
	// Should have invalidated both old and new refs.
	if len(invalidatedRefs) < 2 {
		t.Errorf("expected at least 2 invalidateHosts calls, got %d: %v", len(invalidatedRefs), invalidatedRefs)
	}
	// refreshLayer should have been called 3 times:
	// 1. with old ref (cached hosts), 2. with old ref (after invalidation), 3. with new ref (fallback)
	if refreshCount != 3 {
		t.Errorf("expected 3 refreshLayer calls, got %d", refreshCount)
	}
}

func TestCheckSucceedsWithFallbackRefWithoutInvalidateHosts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	refreshCount := 0

	bl := &breakableLayer{}
	bl.success = false

	oldRef := reference.Spec{Locator: "registry.example.com/repo/image", Object: "old-tag"}
	newRef := reference.Spec{Locator: "registry.example.com/repo/image", Object: "new-tag"}

	fs := &filesystem{
		layer: map[string]layer.Layer{
			"test": bl,
		},
		getSources: func(labels map[string]string) ([]source.Source, error) {
			refreshCount++
			refStr := labels[ctdsnapshotters.TargetRefLabel]
			ref, _ := reference.Parse(refStr)
			// Only succeed when using the new (fallback) ref.
			if ref.String() == newRef.String() {
				bl.success = true
			}
			return []source.Source{
				{Name: ref},
			}, nil
		},
		// invalidateHosts is intentionally nil.
	}

	ctx = source.WithFallbackImageRef(ctx, newRef.String())

	labels := map[string]string{
		ctdsnapshotters.TargetRefLabel: oldRef.String(),
	}

	if err := fs.Check(ctx, "test", labels); err != nil {
		t.Errorf("connection failed with fallback ref (no invalidateHosts); wanted to succeed: %v", err)
	}
	// refreshLayer should have been called 2 times:
	// 1. with old ref (fails), 2. with new ref (fallback, succeeds)
	if refreshCount != 2 {
		t.Errorf("expected 2 refreshLayer calls, got %d", refreshCount)
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
