package ci

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type imageStep struct {
	Uses string         `yaml:"uses"`
	Run  string         `yaml:"run"`
	With map[string]any `yaml:"with"`
}

type imageJob struct {
	RunsOn      string            `yaml:"runs-on"`
	Permissions map[string]string `yaml:"permissions"`
	Strategy    struct {
		Matrix struct {
			Include []struct {
				Platform string `yaml:"platform"`
				Runner   string `yaml:"runner"`
			} `yaml:"include"`
		} `yaml:"matrix"`
	} `yaml:"strategy"`
	Steps []imageStep `yaml:"steps"`
}

func imageConfig(t *testing.T) ([]byte, map[string]any, map[string]imageJob) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "workflows", "docker-publish.yml"))
	if err != nil {
		t.Fatalf("PR branch must provide a dispatchable preview image workflow: %v", err)
	}
	var config struct {
		On          map[string]any      `yaml:"on"`
		Permissions map[string]string   `yaml:"permissions"`
		Jobs        map[string]imageJob `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if config.Permissions["contents"] != "read" || config.Permissions["packages"] != "" {
		t.Fatalf("publish permission must be job scoped: %v", config.Permissions)
	}
	return data, config.On, config.Jobs
}

func TestPRImageWorkflowManualOnlyAndGHCRScoped(t *testing.T) {
	data, events, jobs := imageConfig(t)
	if len(events) != 1 {
		t.Fatalf("preview workflow must not publish on push/release: %v", events)
	}
	if _, ok := events["workflow_dispatch"]; !ok {
		t.Fatal("missing manual dispatch")
	}
	text := string(data)
	if !strings.Contains(text, "ghcr.io/mininglamp-oss/octo-im") {
		t.Fatal("preview images must target this fork's GHCR package")
	}
	for _, forbidden := range []string{"DOCKERHUB_TOKEN", "ALIYUNHUB_TOKEN", "wukongim/wukongim", "type=semver", "latest=auto"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("preview workflow contains unsafe publisher setting %q", forbidden)
		}
	}
	for _, name := range []string{"build", "merge"} {
		if jobs[name].Permissions["packages"] != "write" {
			t.Errorf("%s must be able to publish GHCR images", name)
		}
	}
	if jobs["validate"].Permissions["packages"] != "" {
		t.Fatal("validation must not receive a package write token")
	}
	pinned := regexp.MustCompile(`^[^@]+@[0-9a-f]{40}$`)
	for job, spec := range jobs {
		for _, step := range spec.Steps {
			if step.Uses != "" && !pinned.MatchString(step.Uses) {
				t.Errorf("%s uses an unpinned action: %s", job, step.Uses)
			}
		}
	}
}

func TestPRImageWorkflowNativePlatformsAndImmutableCheckout(t *testing.T) {
	_, _, jobs := imageConfig(t)
	build := jobs["build"]
	if build.RunsOn != "${{ matrix.runner }}" || len(build.Strategy.Matrix.Include) != 2 {
		t.Fatal("build needs separate native AMD64 and ARM64 runners")
	}
	want := map[string]string{"linux/amd64": "ubuntu-24.04", "linux/arm64": "ubuntu-24.04-arm"}
	for _, entry := range build.Strategy.Matrix.Include {
		if want[entry.Platform] != entry.Runner {
			t.Errorf("non-native platform/runner pair: %+v", entry)
		}
		delete(want, entry.Platform)
	}
	if len(want) != 0 {
		t.Fatalf("missing native platforms: %v", want)
	}
	checkout, dockerfile := false, false
	for _, step := range build.Steps {
		if strings.HasPrefix(step.Uses, "actions/checkout@") {
			checkout = step.With["ref"] == "${{ github.sha }}" && step.With["persist-credentials"] == false
		}
		if strings.HasPrefix(step.Uses, "docker/build-push-action@") {
			dockerfile = step.With["file"] == "./Dockerfile.pr" && step.With["platforms"] == "${{ matrix.platform }}"
		}
	}
	if !checkout || !dockerfile {
		t.Fatal("build must check out the event SHA without credentials and use Dockerfile.pr")
	}
	var validation string
	for _, step := range jobs["validate"].Steps {
		validation += step.Run
	}
	for _, required := range []string{"test-pr70-${GITHUB_SHA:0:12}", `"$INPUT_TAG" != "$tag"`, `"$revision" != "$GITHUB_SHA"`, "git status --porcelain"} {
		if !strings.Contains(validation, required) {
			t.Errorf("missing preview source/tag guard %q", required)
		}
	}
}

func TestPRImageDockerfilePinsRuntimeAndEmbeddedAssets(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "Dockerfile.pr"))
	if err != nil {
		t.Fatalf("missing reproducible preview image Dockerfile: %v", err)
	}
	for _, required := range []string{
		"FROM golang:1.23.0 AS build", "GOTOOLCHAIN=local", "CGO_ENABLED=0",
		"FROM alpine:3.23.4", "yarn@1.22.22", "--frozen-lockfile",
		"COPY --from=frontend /frontend/chatdemo/dist ./demo/chatdemo/dist",
		"COPY --from=frontend /frontend/web/dist ./web/dist",
		"main.Commit=${SOURCE_REVISION}", "main.TreeState=clean",
		"--ignoreMissingConfig=true",
	} {
		if !strings.Contains(string(data), required) {
			t.Errorf("missing image build invariant %q", required)
		}
	}
}
