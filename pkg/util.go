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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"stash.appscode.dev/apimachinery/apis"
	api_v1beta1 "stash.appscode.dev/apimachinery/apis/stash/v1beta1"
	stash "stash.appscode.dev/apimachinery/client/clientset/versioned"
	"stash.appscode.dev/apimachinery/pkg/invoker"
	"stash.appscode.dev/apimachinery/pkg/restic"

	shell "gomodules.xyz/go-sh"
	"gomodules.xyz/pointer"
	apps "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	restconfig "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/klog/v2"
	kutil "kmodules.xyz/client-go"
	kmapi "kmodules.xyz/client-go/api/v1"
	apps_util "kmodules.xyz/client-go/apps/v1"
	core_util "kmodules.xyz/client-go/core/v1"
	"kmodules.xyz/client-go/meta"
	appcatalog "kmodules.xyz/custom-resources/apis/appcatalog/v1alpha1"
	appcatalog_cs "kmodules.xyz/custom-resources/client/clientset/versioned"
)

const (
	StashEtcd          = "/stash-etcd"
	EtcdUser           = "username"
	EtcdPassword       = "password"
	EtcdBackupFile     = "snapshot.db"
	EtcdBackupCMD      = "etcdctl"
	EtcdRestoreCMD     = "etcdctl"
	EtcdScratchDir     = "/tmp"
	EtcdCAcertFile     = "root.pem"
	EtcdClientCertFile = "client.pem"
	EtcdClientKeyFile  = "client-key.pem"
	RestoreDirSuffix   = "restore"
	RestorePodPrefix   = "restore"
)

type options struct {
	kubeClient    kubernetes.Interface
	stashClient   stash.Interface
	catalogClient appcatalog_cs.Interface
	config        *restconfig.Config

	addonImage string

	repositorySecretName string

	namespace           string
	backupSessionName   string
	appBindingName      string
	appBindingNamespace string
	etcdArgs            string
	waitTimeout         int32
	outputDir           string
	storageSecret       kmapi.ObjectReference

	etcd etcd

	workloadKind string
	workloadName string

	invokerKind string
	invokerName string

	invoker invoker.RestoreInvoker

	setupOptions   restic.SetupOptions
	backupOptions  restic.BackupOptions
	restoreOptions restic.RestoreOptions
}

type etcd struct {
	initialCluster      string
	initialClusterToken string
	dataDir             string
	interimDataDir      string
	endpoint            string
}
type Shell interface {
	SetEnv(key, value string)
}

type SessionWrapper struct {
	*shell.Session
}

func NewSessionWrapper() *SessionWrapper {
	return &SessionWrapper{
		shell.NewSession(),
	}
}

func (wrapper *SessionWrapper) SetEnv(key, value string) {
	wrapper.Session.SetEnv(key, value)
}

func (opt *options) waitForMemberReady(creds []string, endpoint string) error {
	opt.etcd.endpoint = endpoint
	err := opt.waitForDBReady(creds)
	return err
}

func (opt *options) waitForDBReady(creds []string) error {
	sh := NewSessionWrapper()
	sh.SetEnv("ETCDCTL_API", "3")

	args := make([]string, 0)
	args = append(args, creds...)

	args = append(args, "--endpoints", opt.etcd.endpoint, "endpoint", "health")

	return wait.PollImmediate(time.Second*5, time.Second*time.Duration(opt.waitTimeout), func() (bool, error) {
		err := sh.Command(EtcdBackupCMD, args).Run()
		if err != nil {
			return false, nil
		}
		return true, nil
	})
}

func clearDir(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("unable to clean datadir: %v. Reason: %v", dir, err)
	}
	return os.MkdirAll(dir, os.ModePerm)
}

func (opt *options) restoreEtcdMember(memberPod corev1.Pod, args []string) (*corev1.Pod, error) {
	restoreArgs := opt.getRestoreArgs()
	restoreArgs = append(restoreArgs, args...)
	klog.Infoln("Creating restore interim pod...")
	restorePod, err := opt.createRestorePods(memberPod, restoreArgs)
	if err != nil {
		return nil, err
	}

	return restorePod, nil
}

func (opt *options) createRestorePods(memberPod corev1.Pod, args []string) (*corev1.Pod, error) {
	volumes := make([]corev1.Volume, 0)

	// Getting the volume which is mounted containing the data dir
	var dataVolume, dataVolumeMountPath, pvcName string
	for _, con := range memberPod.Spec.Containers {
		for _, vol := range con.VolumeMounts {
			if strings.HasPrefix(opt.etcd.dataDir, vol.MountPath) {
				dataVolume = vol.Name
				dataVolumeMountPath = vol.MountPath
				break
			}
		}
		if dataVolume != "" {
			break
		}
	}

	if dataVolume == "" {
		return nil, errors.New("No volume is mounted to the provided data directory for the member pod- " + memberPod.Name)
	}

	for _, vol := range memberPod.Spec.Volumes {
		if vol.Name == dataVolume {
			pvcName = vol.VolumeSource.PersistentVolumeClaim.ClaimName
			break
		}
	}

	vol := corev1.Volume{
		Name: dataVolume,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
		},
	}
	volumes = append(volumes, vol)

	vol = corev1.Volume{
		Name: apis.TmpDirVolumeName,
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium:    opt.invoker.GetTargetInfo()[0].TempDir.Medium,
				SizeLimit: opt.invoker.GetTargetInfo()[0].TempDir.SizeLimit,
			},
		},
	}
	volumes = append(volumes, vol)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      meta.ValidNameWithPrefix(RestorePodPrefix, memberPod.Name),
			Namespace: opt.namespace,
			Labels:    opt.invoker.GetLabels(),
		},
		Spec: corev1.PodSpec{
			Volumes: volumes,
			Containers: []corev1.Container{
				{
					Name:    meta.ValidNameWithPrefix(RestorePodPrefix, memberPod.Name),
					Image:   opt.addonImage,
					Command: args,
					Ports: []corev1.ContainerPort{
						{
							Name:          "client",
							ContainerPort: 2379,
						},
						{
							Name:          "peer",
							ContainerPort: 2380,
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      dataVolume,
							MountPath: dataVolumeMountPath,
						},
						{
							Name:      apis.TmpDirVolumeName,
							MountPath: apis.TmpDirMountPath,
						},
					},
					ImagePullPolicy: corev1.PullIfNotPresent,
					SecurityContext: opt.invoker.GetTargetInfo()[0].RuntimeSettings.Container.SecurityContext,
				},
			},
			RestartPolicy:      corev1.RestartPolicyOnFailure,
			ServiceAccountName: opt.invokerName,
		},
	}

	createdPod, _, err := core_util.CreateOrPatchPod(context.TODO(), opt.kubeClient, pod.ObjectMeta, func(in *corev1.Pod) *corev1.Pod {
		// Set RestoreSession as owner of the PVC so that it get deleted when the respective owner is deleted.
		core_util.EnsureOwnerReference(&in.ObjectMeta, opt.invoker.GetOwnerRef())
		in.Spec = pod.Spec
		return in
	}, metav1.PatchOptions{})
	if err != nil {
		return nil, err
	}

	err = waitUntilPodReady(opt.kubeClient, createdPod.ObjectMeta)
	if err != nil {
		return nil, err
	}

	return createdPod, nil
}

func (opt *options) replaceOldDataWithRestoredData(restorePods []corev1.Pod) error {
	for _, pod := range restorePods {
		command := []string{StashEtcd, "replace"}
		command = append(command, "--data-dir="+opt.etcd.dataDir)

		execOut, err := opt.execCommandOnPod(&pod, pod.ObjectMeta.Name, command)
		if err != nil {
			return err
		}
		klog.Infoln(execOut)
	}
	return nil
}

func (opt *options) scaleDownWorkload() error {
	switch opt.workloadKind {
	case apis.KindStatefulSet:
		ss, err := opt.kubeClient.AppsV1().StatefulSets(opt.namespace).Get(context.TODO(), opt.workloadName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		_, _, err = apps_util.PatchStatefulSet(
			context.TODO(),
			opt.kubeClient,
			ss,
			func(obj *apps.StatefulSet) *apps.StatefulSet {
				obj.Annotations[apis.AnnotationOldReplica] = strconv.Itoa(int(*ss.Spec.Replicas))
				obj.Spec.Replicas = pointer.Int32P(0)
				return obj
			},
			metav1.PatchOptions{},
		)
		if err != nil {
			return err
		}
	case apis.KindPod:

	default:
	}
	err := opt.waitUntilScalingCompleted()
	if err != nil {
		return err
	}

	return nil
}

func (opt *options) scaleUpWorkload(numberOfMembersInEtcdCluster int32) error {
	switch opt.workloadKind {
	case apis.KindStatefulSet:
		ss, err := opt.kubeClient.AppsV1().StatefulSets(opt.namespace).Get(context.TODO(), opt.workloadName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		_, _, err = apps_util.PatchStatefulSet(
			context.TODO(),
			opt.kubeClient,
			ss,
			func(obj *apps.StatefulSet) *apps.StatefulSet {
				obj.Annotations[apis.AnnotationOldReplica] = strconv.Itoa(int(*ss.Spec.Replicas))
				obj.Spec.Replicas = &numberOfMembersInEtcdCluster
				return obj
			},
			metav1.PatchOptions{},
		)
		if err != nil {
			return err
		}
	case apis.KindPod:
	default:
	}

	err := opt.waitUntilScalingCompleted()
	if err != nil {
		return err
	}

	return nil
}

func (opt *options) getEtcdMemberPods() ([]corev1.Pod, error) {
	switch opt.workloadKind {
	case apis.KindStatefulSet:
		ss, err := opt.kubeClient.AppsV1().StatefulSets(opt.namespace).Get(context.TODO(), opt.workloadName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}

		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
			MatchLabels: ss.Spec.Selector.MatchLabels,
		})
		if err != nil {
			return nil, err
		}

		pods, err := opt.kubeClient.CoreV1().Pods(opt.namespace).List(context.TODO(), metav1.ListOptions{LabelSelector: selector.String()})
		if err != nil {
			return nil, err
		}
		return pods.Items, nil

	case apis.KindPod:
		pod, err := opt.kubeClient.CoreV1().Pods(opt.namespace).Get(context.TODO(), opt.workloadName, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return []corev1.Pod{*pod}, nil
	default:
		return nil, errors.New("Could not find any member for etcd server")
	}
}

func (opt *options) execCommandOnPod(pod *corev1.Pod, containerName string, command []string) ([]byte, error) {
	var (
		execOut bytes.Buffer
		execErr bytes.Buffer
	)

	req := opt.kubeClient.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(pod.Name).
		Namespace(pod.Namespace).
		SubResource("exec").
		Timeout(5 * time.Minute)
	req.VersionedParams(&corev1.PodExecOptions{
		Container: containerName,
		Command:   command,
		Stdout:    true,
		Stderr:    true,
	}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(opt.config, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("failed to init executor: %v", err)
	}

	err = exec.Stream(remotecommand.StreamOptions{
		Stdout: &execOut,
		Stderr: &execErr,
		Tty:    true,
	})

	if err != nil {
		return nil, fmt.Errorf("Could not execute: %v, reason: %s", err, execErr.String())
	}

	return execOut.Bytes(), nil
}

func waitUntilPodReady(c kubernetes.Interface, meta metav1.ObjectMeta) error {
	return wait.PollImmediate(kutil.RetryInterval, 5*time.Minute, func() (bool, error) {
		if obj, err := c.CoreV1().Pods(meta.Namespace).Get(context.TODO(), meta.Name, metav1.GetOptions{}); err == nil {
			return obj.Status.Phase == corev1.PodRunning, nil
		}
		return false, nil
	})
}

func (opt *options) waitUntilRestoreComplete(numberOfMembersInEtcdCluster int) error {
	return wait.PollImmediate(1*time.Second, 2*time.Hour, func() (bool, error) {
		restoreSession, err := opt.stashClient.StashV1beta1().RestoreSessions(opt.namespace).Get(context.TODO(), opt.invokerName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}

		if len(restoreSession.Status.Stats) != numberOfMembersInEtcdCluster {
			return false, nil
		}

		for _, stat := range restoreSession.Status.Stats {
			if stat.Phase == api_v1beta1.HostRestoreRunning {
				return false, nil
			}
			if stat.Phase != api_v1beta1.HostRestoreSucceeded {
				return false, fmt.Errorf("restore process failed for host: %s , Reason: %s", stat.Hostname, stat.Error)
			}
		}

		return true, nil
	})
}

func (opt *options) waitUntilScalingCompleted() error {
	switch opt.workloadKind {
	case apis.KindStatefulSet:
		return wait.PollImmediate(kutil.RetryInterval, time.Second*time.Duration(opt.waitTimeout), func() (bool, error) {
			ss, err := opt.kubeClient.AppsV1().StatefulSets(opt.namespace).Get(context.TODO(), opt.workloadName, metav1.GetOptions{})
			if err != nil {
				return false, err
			}

			if *ss.Spec.Replicas != ss.Status.ReadyReplicas {
				return false, nil
			}

			return true, nil
		})
	case apis.KindPod:
	default:
	}
	return nil
}

func (opt *options) getRestoreArgs() []string {
	args := []string{StashEtcd, "restore-member"}
	args = append(args, "--appbinding="+opt.appBindingName)
	args = append(args, "--namespace="+opt.namespace)
	args = append(args, "--storage-secret-name="+opt.storageSecret.Name)
	args = append(args, "--storage-secret-namespace="+opt.storageSecret.Namespace)
	args = append(args, "--kubeconfig=")
	args = append(args, "--provider="+opt.setupOptions.Provider)
	args = append(args, "--bucket="+opt.setupOptions.Bucket)
	args = append(args, "--endpoint="+opt.setupOptions.Endpoint)
	args = append(args, "--region="+opt.setupOptions.Region)
	args = append(args, "--path="+opt.setupOptions.Path)
	args = append(args, "--scratch-dir="+EtcdScratchDir)
	args = append(args, "--enable-cache="+fmt.Sprintf("%v", opt.setupOptions.EnableCache))
	args = append(args, "--max-connections="+fmt.Sprintf("%v", opt.setupOptions.MaxConnections))
	args = append(args, "--source-hostname="+opt.restoreOptions.SourceHost)
	args = append(args, "--invoker-name="+opt.invokerName)
	args = append(args, "--invoker-kind="+opt.invokerKind)
	args = append(args, fmt.Sprintf("--snapshots=%s", strings.Join(opt.restoreOptions.Snapshots, ",")))
	return args
}

func (opt *options) getCredential(appBinding *appcatalog.AppBinding) ([]string, error) {
	args := make([]string, 0)

	if appBinding.Spec.Secret == nil {
		return args, nil
	}

	appBindingSecret, err := opt.kubeClient.CoreV1().Secrets(appBinding.Namespace).Get(context.TODO(), appBinding.Spec.Secret.Name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	// transform secret
	err = appBinding.TransformSecret(opt.kubeClient, appBindingSecret.Data)
	if err != nil {
		return nil, err
	}

	if appBinding.Spec.ClientConfig.CABundle != nil {
		if err := os.WriteFile(filepath.Join(opt.setupOptions.ScratchDir, EtcdCAcertFile), appBinding.Spec.ClientConfig.CABundle, os.ModePerm); err != nil {
			return nil, err
		}
		tlsArgs := fmt.Sprintf("--cacert=%v", filepath.Join(opt.setupOptions.ScratchDir, EtcdCAcertFile))
		args = append(args, tlsArgs)

		cert, certExist := appBindingSecret.Data[EtcdClientCertFile]
		key, keyExist := appBindingSecret.Data[EtcdClientKeyFile]

		if certExist && keyExist {
			if err := os.WriteFile(filepath.Join(opt.setupOptions.ScratchDir, EtcdClientCertFile), cert, os.ModePerm); err != nil {
				return nil, err
			}
			tlsArgs = fmt.Sprintf("--cert=%v", filepath.Join(opt.setupOptions.ScratchDir, EtcdClientCertFile))
			args = append(args, tlsArgs)

			if err := os.WriteFile(filepath.Join(opt.setupOptions.ScratchDir, EtcdClientKeyFile), key, os.ModePerm); err != nil {
				return nil, err
			}
			tlsArgs = fmt.Sprintf("--key=%v", filepath.Join(opt.setupOptions.ScratchDir, EtcdClientKeyFile))
			args = append(args, tlsArgs)
		} else {
			return nil, errors.New("Client cert and client key needed for TLS secured operation")
		}

		return args, nil
	}

	authKeys := fmt.Sprintf("%s:%s",
		string(appBindingSecret.Data[EtcdUser]),
		string(appBindingSecret.Data[EtcdPassword]))

	args = append(args, "--user", authKeys)

	return args, nil
}

func (opt *options) getEndpoint(appBinding *appcatalog.AppBinding) (string, error) {
	endpoint, err := appBinding.Hostname()
	if err != nil {
		return "", err
	}

	port, err := appBinding.Port()
	if err != nil {
		return "", err
	}

	if port != 0 {
		endpoint = endpoint + ":" + strconv.Itoa(int(appBinding.Spec.ClientConfig.Service.Port))
	}
	return endpoint, nil
}
