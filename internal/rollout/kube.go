package rollout

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// Kube implements Cluster against two Deployments.
type Kube struct {
	Client      kubernetes.Interface
	Namespace   string
	Stable      string // stable Deployment name
	Canary      string // canary Deployment name
	ReadyWithin time.Duration
}

func (k *Kube) get(ctx context.Context, name string) (*appsv1.Deployment, error) {
	return k.Client.AppsV1().Deployments(k.Namespace).Get(ctx, name, metav1.GetOptions{})
}

func (k *Kube) StableReplicas(ctx context.Context) (int32, error) {
	d, err := k.get(ctx, k.Stable)
	if err != nil {
		return 0, err
	}
	if d.Spec.Replicas == nil {
		return 1, nil
	}
	return *d.Spec.Replicas, nil
}

func (k *Kube) update(ctx context.Context, name string, mutate func(*appsv1.Deployment)) error {
	d, err := k.get(ctx, name)
	if err != nil {
		return err
	}
	mutate(d)
	_, err = k.Client.AppsV1().Deployments(k.Namespace).Update(ctx, d, metav1.UpdateOptions{})
	return err
}

// waitReady waits until the Deployment's observed generation is current
// and all desired replicas are updated and available.
func (k *Kube) waitReady(ctx context.Context, name string) error {
	ctx, cancel := context.WithTimeout(ctx, k.ReadyWithin)
	defer cancel()
	for {
		d, err := k.get(ctx, name)
		if err == nil {
			want := int32(1)
			if d.Spec.Replicas != nil {
				want = *d.Spec.Replicas
			}
			st := d.Status
			if st.ObservedGeneration >= d.Generation && st.UpdatedReplicas == want && st.AvailableReplicas == want && st.Replicas == want {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("deployment %s not ready within %s", name, k.ReadyWithin)
		case <-time.After(2 * time.Second):
		}
	}
}

func (k *Kube) SetCanary(ctx context.Context, image string, env map[string]string, n int32) error {
	if err := k.update(ctx, k.Canary, func(d *appsv1.Deployment) {
		d.Spec.Replicas = &n
		c := &d.Spec.Template.Spec.Containers[0]
		c.Image = image
		for name, val := range env {
			found := false
			for i := range c.Env {
				if c.Env[i].Name == name {
					c.Env[i].Value, c.Env[i].ValueFrom, found = val, nil, true
				}
			}
			if !found {
				c.Env = append(c.Env, corev1.EnvVar{Name: name, Value: val})
			}
		}
	}); err != nil {
		return err
	}
	return k.waitReady(ctx, k.Canary)
}

func (k *Kube) Promote(ctx context.Context, image string) error {
	d, err := k.get(ctx, k.Stable)
	if err != nil {
		return err
	}
	prev := d.Spec.Template.Spec.Containers[0].Image
	// Carry the canary's environment (model version, settings) over too.
	c, err := k.get(ctx, k.Canary)
	if err != nil {
		return err
	}
	if err := k.update(ctx, k.Stable, func(d *appsv1.Deployment) {
		d.Spec.Template.Spec.Containers[0].Image = image
		// TRACK comes from the pod label via the downward API, so copying
		// the env verbatim keeps each Deployment's own track.
		d.Spec.Template.Spec.Containers[0].Env = c.Spec.Template.Spec.Containers[0].Env
	}); err != nil {
		return err
	}
	if err := k.waitReady(ctx, k.Stable); err != nil {
		// Undo: restore the previous stable image.
		_ = k.update(context.Background(), k.Stable, func(d *appsv1.Deployment) { d.Spec.Template.Spec.Containers[0].Image = prev })
		return err
	}
	return nil
}

func (k *Kube) Annotate(ctx context.Context, status, reason string) error {
	patch, _ := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]string{
		"rollout.gpusched.dev/status": status, "rollout.gpusched.dev/reason": reason,
		"rollout.gpusched.dev/at": time.Now().UTC().Format(time.RFC3339)}}})
	_, err := k.Client.AppsV1().Deployments(k.Namespace).Patch(ctx, k.Canary, types.MergePatchType, patch, metav1.PatchOptions{})
	return err
}

// Prometheus implements Metrics with the HTTP query API.
type Prometheus struct {
	URL    string
	Client *http.Client
}

func (p *Prometheus) Query(ctx context.Context, q string) (float64, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL+"/api/v1/query?query="+url.QueryEscape(q), nil)
	if err != nil {
		return 0, false, err
	}
	c := p.Client
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0, false, err
	}
	defer resp.Body.Close()
	var body struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, false, err
	}
	if body.Status != "success" {
		return 0, false, fmt.Errorf("prometheus: %s", body.Error)
	}
	if len(body.Data.Result) == 0 {
		return 0, false, nil
	}
	s, _ := body.Data.Result[0].Value[1].(string)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false, err
	}
	return v, true, nil
}
