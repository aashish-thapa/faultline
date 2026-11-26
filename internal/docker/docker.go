// Package docker drives the docker CLI. Arguments are passed as argv, never through a shell.
package docker

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// Run executes `docker args...` and returns trimmed combined output.
func Run(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	text := strings.TrimSpace(string(out))
	if err != nil {
		return text, fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, text)
	}
	return text, nil
}

// InNetns runs a command inside container's network namespace using a
// privileged sidecar, so the target image needs no networking tools of its own.
func InNetns(ctx context.Context, sidecarImage, container string, cmd ...string) (string, error) {
	args := append([]string{"run", "--rm", "--net", "container:" + container, "--cap-add", "NET_ADMIN", sidecarImage}, cmd...)
	return Run(ctx, args...)
}
