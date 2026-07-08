package k8s

import (
	"context"
	"fmt"

	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// PrimaryIngressName is the Ingress a project's always-on primary
// deployment is reachable through (ADR 003 §15).
const PrimaryIngressName = "primary"

// EnsureProjectIngress idempotently creates or updates the Ingress routing
// host to serviceName:servicePort in namespace, with TLS terminated via
// cert-manager (annotation referencing certIssuerName, cert stored in
// tlsSecretName) — cert-manager itself issues/rotates the certificate; the
// Orchestrator only ever declares the Ingress.
func EnsureProjectIngress(
	ctx context.Context,
	clientset kubernetes.Interface,
	namespace, host, serviceName string,
	servicePort int32,
	ingressClassName, tlsSecretName, certIssuerName string,
) error {
	pathType := networkingv1.PathTypePrefix

	ingress := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      PrimaryIngressName,
			Namespace: namespace,
			Annotations: map[string]string{
				"cert-manager.io/cluster-issuer": certIssuerName,
			},
		},
		Spec: networkingv1.IngressSpec{
			IngressClassName: &ingressClassName,
			TLS: []networkingv1.IngressTLS{
				{
					Hosts:      []string{host},
					SecretName: tlsSecretName,
				},
			},
			Rules: []networkingv1.IngressRule{
				{
					Host: host,
					IngressRuleValue: networkingv1.IngressRuleValue{
						HTTP: &networkingv1.HTTPIngressRuleValue{
							Paths: []networkingv1.HTTPIngressPath{
								{
									Path:     "/",
									PathType: &pathType,
									Backend: networkingv1.IngressBackend{
										Service: &networkingv1.IngressServiceBackend{
											Name: serviceName,
											Port: networkingv1.ServiceBackendPort{
												Number: servicePort,
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	_, err := clientset.NetworkingV1().Ingresses(namespace).Create(ctx, ingress, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create ingress %s in namespace %s: %w", PrimaryIngressName, namespace, err)
	}

	// Kubernetes requires the current resourceVersion for an update
	// (optimistic concurrency) — Get before Update rather than blindly
	// retrying Create's object.
	existing, err := clientset.NetworkingV1().Ingresses(namespace).Get(ctx, PrimaryIngressName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to fetch existing ingress %s in namespace %s: %w", PrimaryIngressName, namespace, err)
	}
	existing.Annotations = ingress.Annotations
	existing.Spec = ingress.Spec

	if _, err := clientset.NetworkingV1().Ingresses(namespace).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update ingress %s in namespace %s: %w", PrimaryIngressName, namespace, err)
	}
	return nil
}
