/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Free Trial License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Free-Trial-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pkg

import (
	"context"

	"stash.appscode.dev/apimachinery/apis"
	api_v1alpha1 "stash.appscode.dev/apimachinery/apis/stash/v1alpha1"
	api_v1beta1 "stash.appscode.dev/apimachinery/apis/stash/v1beta1"

	corev1 "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core_util "kmodules.xyz/client-go/core/v1"
	rbac_util "kmodules.xyz/client-go/rbac/v1"
	appcatalog "kmodules.xyz/custom-resources/apis/appcatalog/v1alpha1"
)

func (opt *options) ensureRestorePodRBAC() error {
	err := opt.ensureServiceAccount()
	if err != nil {
		return err
	}
	err = opt.ensureRole()
	if err != nil {
		return err
	}
	err = opt.ensureRoleBinding()

	return err
}

func (opt *options) ensureServiceAccount() error {
	// Creating serviceaccount for etcd interim pods
	saMeta := metav1.ObjectMeta{
		Name:      opt.invokerName,
		Namespace: opt.namespace,
		Labels:    opt.invoker.Labels,
	}

	_, _, err := core_util.CreateOrPatchServiceAccount(
		context.TODO(),
		opt.kubeClient,
		saMeta,
		func(in *corev1.ServiceAccount) *corev1.ServiceAccount {
			core_util.EnsureOwnerReference(&in.ObjectMeta, opt.invoker.OwnerRef)
			in.Labels = opt.invoker.Labels
			return in
		},
		metav1.PatchOptions{},
	)

	return err
}

func (opt *options) ensureRole() error {
	// Creating role for etcd interim pods
	meta := metav1.ObjectMeta{
		Name:      opt.invokerName,
		Namespace: opt.namespace,
		Labels:    opt.invoker.Labels,
	}

	_, _, err := rbac_util.CreateOrPatchRole(context.TODO(), opt.kubeClient, meta, func(in *rbac.Role) *rbac.Role {
		core_util.EnsureOwnerReference(&in.ObjectMeta, opt.invoker.OwnerRef)
		in.Rules = []rbac.PolicyRule{
			{
				APIGroups: []string{api_v1beta1.SchemeGroupVersion.Group},
				Resources: []string{"*"},
				Verbs:     []string{"*"},
			},
			{
				APIGroups: []string{api_v1alpha1.SchemeGroupVersion.Group},
				Resources: []string{"*"},
				Verbs:     []string{"*"},
			},
			{
				APIGroups: []string{appcatalog.SchemeGroupVersion.Group},
				Resources: []string{appcatalog.ResourceApps},
				Verbs:     []string{"get"},
			},
			{
				APIGroups: []string{corev1.SchemeGroupVersion.Group},
				Resources: []string{"secrets"},
				Verbs:     []string{"get"},
			},
		}
		return in

	}, metav1.PatchOptions{})

	return err
}

func (opt *options) ensureRoleBinding() error {
	meta := metav1.ObjectMeta{
		Namespace: opt.namespace,
		Name:      opt.invokerName,
		Labels:    opt.invoker.Labels,
	}

	_, _, err := rbac_util.CreateOrPatchRoleBinding(context.TODO(), opt.kubeClient, meta, func(in *rbac.RoleBinding) *rbac.RoleBinding {
		core_util.EnsureOwnerReference(&in.ObjectMeta, opt.invoker.OwnerRef)

		in.RoleRef = rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     apis.KindRole,
			Name:     opt.invokerName,
		}
		in.Subjects = []rbac.Subject{
			{
				Kind:      rbac.ServiceAccountKind,
				Name:      opt.invokerName,
				Namespace: opt.namespace,
			},
		}
		return in
	}, metav1.PatchOptions{})

	return err
}
