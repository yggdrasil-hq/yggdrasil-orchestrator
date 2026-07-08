// Package k8s wires the Orchestrator to its single target Kubernetes cluster
// (ADR 003 — one cluster per Orchestrator instance) and provisions
// per-project resources within it.
package k8s

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// NewClient builds a Kubernetes clientset for the Orchestrator's single
// target cluster. It tries in-cluster config first (how a self-hosted or
// managed Orchestrator running inside its target cluster will authenticate),
// falling back to KUBECONFIG / the default kubeconfig path for local
// development.
func NewClient() (*kubernetes.Clientset, error) {
	config, err := RESTConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

// RESTConfig resolves the REST config for the Orchestrator's single target
// cluster, for callers (e.g. internal/helm) that need it directly rather
// than through a typed Kubernetes clientset.
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
