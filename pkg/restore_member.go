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
	"fmt"
	"path/filepath"

	"stash.appscode.dev/apimachinery/apis"
	api_v1beta1 "stash.appscode.dev/apimachinery/apis/stash/v1beta1"
	stash "stash.appscode.dev/apimachinery/client/clientset/versioned"
	"stash.appscode.dev/apimachinery/pkg/invoker"
	"stash.appscode.dev/apimachinery/pkg/restic"

	"github.com/spf13/cobra"
	"gomodules.xyz/flags"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	appcatalog "kmodules.xyz/custom-resources/apis/appcatalog/v1alpha1"
	appcatalog_cs "kmodules.xyz/custom-resources/client/clientset/versioned"
	v1 "kmodules.xyz/offshoot-api/api/v1"
)

var downloadDir, name, initialAdvertisePeerUrl, initialCluster, initialClusterToken string

func NewCmdRestoreMember() *cobra.Command {
	var (
		masterURL      string
		kubeconfigPath string
		opt            = options{
			setupOptions: restic.SetupOptions{
				ScratchDir:  restic.DefaultScratchDir,
				EnableCache: false,
			},
			waitTimeout: 300,
			restoreOptions: restic.RestoreOptions{
				Host: restic.DefaultHost,
			},
			etcd: etcd{interimDataDir: filepath.Join(apis.TmpDirMountPath, "data")},
		}
	)

	cmd := &cobra.Command{
		Use:               "restore-member",
		Short:             "Download Etcd DB backup from the backend cloud and restore it in the restore-directory",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags.EnsureRequiredFlags(cmd, "appbinding", "provider", "hostname", "data-dir", "storage-secret-name", "storage-secret-namespace")

			// prepare client
			config, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfigPath)
			if err != nil {
				return err
			}
			opt.kubeClient, err = kubernetes.NewForConfig(config)
			if err != nil {
				return err
			}
			opt.catalogClient, err = appcatalog_cs.NewForConfig(config)
			if err != nil {
				return err
			}
			opt.stashClient, err = stash.NewForConfig(config)
			if err != nil {
				return err
			}

			targetRef := api_v1beta1.TargetRef{
				APIVersion: appcatalog.SchemeGroupVersion.String(),
				Kind:       appcatalog.ResourceKindApp,
				Name:       opt.appBindingName,
			}

			//Get the restore invoker information
			opt.invoker, err = invoker.NewRestoreInvoker(opt.kubeClient, opt.stashClient, opt.invokerKind, opt.invokerName, opt.namespace)
			if err != nil {
				return err
			}

			err = opt.updateHostRestoreStatus(targetRef, api_v1beta1.HostRestoreRunning, nil)
			if err != nil {
				return err
			}

			// Download the snapshot from cloud backend into an interim directory
			err = opt.downloadSnapshot(targetRef)
			if err != nil {
				return opt.updateHostRestoreStatus(targetRef, api_v1beta1.HostRestoreFailed, err)
			}

			// Now, restore the downloaded snapshot from interim directory into a sub-directory of this etcd data directory.
			restoreErr := opt.restoreSnapshot()
			err = opt.updateHostRestoreStatus(targetRef, "", restoreErr)
			if err != nil {
				return err
			}

			// Running sleep command for keeping the pod running to execute rest of the restore processes in the interim pods from stash restore job
			sh := NewSessionWrapper()
			return sh.Command("sleep", "7200").Run()
		},
	}
	cmd.Flags().StringVar(&opt.appBindingName, "appbinding", opt.appBindingName, "Name of the app binding")
	cmd.Flags().StringVar(&masterURL, "master", masterURL, "The address of the Kubernetes API server (overrides any value in kubeconfig)")
	cmd.Flags().StringVar(&kubeconfigPath, "kubeconfig", kubeconfigPath, "Path to kubeconfig file with authorization information (the master location is set by the master flag).")
	cmd.Flags().StringVar(&opt.namespace, "namespace", "default", "Namespace of Backup/Restore Session")
	cmd.Flags().StringVar(&opt.storageSecret.Name, "storage-secret-name", opt.storageSecret.Name, "Name of the storage secret")
	cmd.Flags().StringVar(&opt.storageSecret.Namespace, "storage-secret-namespace", opt.storageSecret.Namespace, "Namespace of the storage secret")

	cmd.Flags().StringVar(&opt.setupOptions.Provider, "provider", opt.setupOptions.Provider, "Backend provider (i.e. gcs, s3, azure etc)")
	cmd.Flags().StringVar(&opt.setupOptions.Bucket, "bucket", opt.setupOptions.Bucket, "Name of the cloud bucket/container (keep empty for local backend)")
	cmd.Flags().StringVar(&opt.setupOptions.Endpoint, "endpoint", opt.setupOptions.Endpoint, "Endpoint for s3/s3 compatible backend or REST backend URL")
	cmd.Flags().StringVar(&opt.setupOptions.Region, "region", opt.setupOptions.Region, "Region for s3/s3 compatible backend")
	cmd.Flags().StringVar(&opt.setupOptions.Path, "path", opt.setupOptions.Path, "Directory inside the bucket where backup will be stored")
	cmd.Flags().StringVar(&opt.setupOptions.ScratchDir, "scratch-dir", opt.setupOptions.ScratchDir, "Temporary directory")
	cmd.Flags().BoolVar(&opt.setupOptions.EnableCache, "enable-cache", opt.setupOptions.EnableCache, "Specify whether to enable caching for restic")
	cmd.Flags().Int64Var(&opt.setupOptions.MaxConnections, "max-connections", opt.setupOptions.MaxConnections, "Specify maximum concurrent connections for GCS, Azure and B2 backend")

	cmd.Flags().StringVar(&opt.restoreOptions.Host, "hostname", opt.restoreOptions.Host, "Name of the host machine")
	cmd.Flags().StringVar(&opt.restoreOptions.SourceHost, "source-hostname", opt.restoreOptions.SourceHost, "Name of the host whose data will be restored")
	cmd.Flags().StringSliceVar(&opt.restoreOptions.Snapshots, "snapshots", opt.restoreOptions.Snapshots, "Snapshots to restore")

	cmd.Flags().StringVar(&opt.invokerKind, "invoker-kind", opt.invokerKind, "Type of the restore invoker")
	cmd.Flags().StringVar(&opt.invokerName, "invoker-name", opt.invokerName, "Name of the restore invoker")

	//Restore flags
	cmd.Flags().StringVar(&downloadDir, "download-dir", downloadDir, "Directory where Snapshots get downloaded")
	cmd.Flags().StringVar(&name, "name", name, "Name of the etcd member to be restored")
	cmd.Flags().StringVar(&initialAdvertisePeerUrl, "initial-advertise-peer-urls", initialAdvertisePeerUrl, "Initial advertise peer urls for etcd cluster")
	cmd.Flags().StringVar(&initialCluster, "initial-cluster", initialCluster, "Initial cluster configuration")
	cmd.Flags().StringVar(&initialClusterToken, "initial-cluster-token", initialClusterToken, "Initial cluster token for etcd cluster")
	cmd.Flags().StringVar(&dataDir, "data-dir", dataDir, "Data directory for restoring etcd cluster")

	return cmd
}

func (opt *options) downloadSnapshot(targetRef api_v1beta1.TargetRef) error {
	var err error
	opt.setupOptions.StorageSecret, err = opt.kubeClient.CoreV1().Secrets(opt.storageSecret.Namespace).Get(context.TODO(), opt.storageSecret.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	// apply nice, ionice settings from env
	opt.setupOptions.Nice, err = v1.NiceSettingsFromEnv()
	if err != nil {
		return err
	}
	opt.setupOptions.IONice, err = v1.IONiceSettingsFromEnv()
	if err != nil {
		return err
	}

	resticWrapper, err := restic.NewResticWrapper(opt.setupOptions)
	if err != nil {
		return err
	}

	_, err = resticWrapper.RunRestore(opt.restoreOptions, targetRef)
	if err != nil {
		return err
	}

	return nil
}

func (opt *options) restoreSnapshot() error {
	appBinding, err := opt.catalogClient.AppcatalogV1alpha1().AppBindings(opt.namespace).Get(context.TODO(), opt.appBindingName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	creds, err := opt.getCredential(appBinding)
	if err != nil {
		return err
	}
	restoreArgs := []string{"snapshot", "restore"}
	restoreArgs = append(restoreArgs, creds...)

	restoreArgs = append(restoreArgs, downloadDir)
	restoreArgs = append(restoreArgs, "--name", name)
	restoreArgs = append(restoreArgs, "--initial-cluster", initialCluster)
	restoreArgs = append(restoreArgs, "--initial-cluster-token", initialClusterToken)
	restoreArgs = append(restoreArgs, "--initial-advertise-peer-urls", initialAdvertisePeerUrl)
	restoreArgs = append(restoreArgs, "--data-dir", dataDir)

	sh := NewSessionWrapper()
	sh.SetEnv("ETCDCTL_API", "3")

	err = sh.Command(EtcdRestoreCMD, restoreArgs).Run()

	return err
}

func (opt *options) updateHostRestoreStatus(targetRef api_v1beta1.TargetRef, phase api_v1beta1.HostRestorePhase, err error) error {
	fmt.Printf("target: %v phase: %v err: %v", targetRef, phase, err)
	var errMsg string
	if phase != api_v1beta1.HostRestoreRunning {
		if err != nil {
			phase = api_v1beta1.HostRestoreFailed
			errMsg = err.Error()
		} else {
			phase = api_v1beta1.HostRestoreSucceeded
		}
	}

	return opt.invoker.UpdateStatus(invoker.RestoreInvokerStatus{
		TargetStatus: []api_v1beta1.RestoreMemberStatus{
			{
				Ref: targetRef,
				Stats: []api_v1beta1.HostRestoreStats{
					{
						Hostname: opt.restoreOptions.Host,
						Phase:    phase,
						Error:    errMsg,
					},
				},
			},
		},
	})
}
