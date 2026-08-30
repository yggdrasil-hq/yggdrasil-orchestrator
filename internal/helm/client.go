// Package helm applies a project's Helm chart to its namespace (ADR 003
// §12-13 — imperative `helm upgrade --install`, no GitOps controller) via
// the official Helm Go SDK.
package helm

import (
	"context"
	"fmt"
	"log"

	"helm.sh/helm/v3/pkg/action"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// helmStorageDriver controls where Helm stores release metadata. "secret"
// (Kubernetes Secrets in the release namespace) is Helm 3's default and
// keeps release state alongside everything else in the project's namespace.
const helmStorageDriver = "secret"

// NewConfiguration builds a Helm action.Configuration scoped to namespace,
// using the provided *rest.Config — the per-Organization cluster config a
// deploy job resolved at claim time (ADR 016 item 13). Takes the config
// explicitly rather than deriving it from an ambient kubeconfig, since a
// deploy must target whatever cluster the project's org owns.
func NewConfiguration(restConfig *rest.Config, namespace string) (*action.Configuration, error) {
	cfg := new(action.Configuration)
	getter := &restClientGetter{config: restConfig, namespace: namespace}
	debugLog := func(format string, v ...interface{}) {
		log.Printf("helm: "+format, v...)
	}
	if err := cfg.Init(getter, namespace, helmStorageDriver, debugLog); err != nil {
		return nil, fmt.Errorf("failed to initialize helm configuration: %w", err)
	}
	return cfg, nil
}

func Uninstall(ctx context.Context, cfg *action.Configuration, releaseName string) error {
	_ = ctx
	uninstall := action.NewUninstall(cfg)
	uninstall.Wait = true
	if _, err := uninstall.Run(releaseName); err != nil {
		return fmt.Errorf("failed to uninstall release %s: %w", releaseName, err)
	}
	return nil
}

// restClientGetter adapts an already-resolved *rest.Config to Helm's
// genericclioptions.RESTClientGetter interface, so action.Configuration.Init
// doesn't have to re-derive credentials from a kubeconfig file/CLI flags.
type restClientGetter struct {
	config    *rest.Config
	namespace string
}

func (g *restClientGetter) ToRESTConfig() (*rest.Config, error) {
	return g.config, nil
}

func (g *restClientGetter) ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(g.config)
	if err != nil {
		return nil, err
	}
	return memory.NewMemCacheClient(dc), nil
}

func (g *restClientGetter) ToRESTMapper() (meta.RESTMapper, error) {
	dc, err := g.ToDiscoveryClient()
	if err != nil {
		return nil, err
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(dc)
	return restmapper.NewShortcutExpander(mapper, dc, func(string) {}), nil
}

// ToRawKubeConfigLoader is where Helm's underlying resource builder actually
// resolves "which namespace" for manifests that don't set metadata.namespace
// (the normal case for chart templates) — a bare clientcmd.ClientConfig
// would resolve that to "default", silently deploying every release there
// regardless of the namespace action.Configuration was Init'd with. A
// namespace override short-circuits DirectClientConfig.Namespace() before it
// falls through to validating the (nonexistent) cluster/auth-info in our
// empty synthetic config, which is what makes resources actually land in the
// project's namespace.
func (g *restClientGetter) ToRawKubeConfigLoader() clientcmd.ClientConfig {
	overrides := &clientcmd.ConfigOverrides{
		Context: clientcmdapi.Context{Namespace: g.namespace},
	}
	return clientcmd.NewDefaultClientConfig(clientcmdapi.Config{}, overrides)
}
