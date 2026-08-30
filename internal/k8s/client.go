// Package k8s wires the Orchestrator to the Kubernetes cluster(s) it runs
// jobs on. ADR 003 originally pinned a single cluster per instance; ADR 016
// item 13 supersedes that with dynamic per-Organization cluster resolution —
// the Orchestrator now builds clients from each org's stored kubeconfig
// rather than one static client built once at startup.
package k8s

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Client bundles the typed clientset and the raw REST config for one target
// cluster, so callers that need either (jobs need the clientset, the
// attach/helm paths need the REST config) get both from one resolution.
type Client struct {
	Interface kubernetes.Interface
	Config    *rest.Config
}

// NewClientFromKubeconfig builds a Client (clientset + REST config) from raw
// kubeconfig YAML bytes — the path every job's target cluster now flows
// through, since org kubeconfigs are stored in the DB (ADR 016 item 13).
func NewClientFromKubeconfig(kubeconfig []byte) (*Client, error) {
	config, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to parse kubeconfig: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to build Kubernetes clientset from kubeconfig: %w", err)
	}
	return &Client{Interface: clientset, Config: config}, nil
}

// NewClient builds a Kubernetes clientset for the Orchestrator's target
// cluster using the ambient in-cluster config / KUBECONFIG / default kubeconfig
// — kept solely as a local-dev/test convenience. This is NOT the production
// cluster-resolution path (ADR 016 item 13 supersedes it for real jobs); it
// just lets tests and local tooling connect to a developer cluster.
func NewClient() (*kubernetes.Clientset, error) {
	config, err := RESTConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

// RESTConfig resolves the REST config for the Orchestrator's target cluster
// from in-cluster config, KUBECONFIG, or the default kubeconfig path.
func RESTConfig() (*rest.Config, error) {
	if config, err := rest.InClusterConfig(); err == nil {
		return config, nil
	}

	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("no in-cluster config, KUBECONFIG unset, and no home directory: %w", err)
		}
		kubeconfig = filepath.Join(home, ".kube", "config")
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to load kubeconfig from %s: %w", kubeconfig, err)
	}
	return config, nil
}
