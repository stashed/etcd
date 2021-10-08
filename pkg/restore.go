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
	"strconv"
	"strings"

	"stash.appscode.dev/apimachinery/apis"
	api_v1beta1 "stash.appscode.dev/apimachinery/apis/stash/v1beta1"
	stash "stash.appscode.dev/apimachinery/client/clientset/versioned"
	"stash.appscode.dev/apimachinery/pkg/conditions"
	"stash.appscode.dev/apimachinery/pkg/invoker"
	"stash.appscode.dev/apimachinery/pkg/restic"

	"github.com/spf13/cobra"
	license "go.bytebuilders.dev/license-verifier/kubernetes"
	"gomodules.xyz/flags"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	appcatalog "kmodules.xyz/custom-resources/apis/appcatalog/v1alpha1"
	appcatalog_cs "kmodules.xyz/custom-resources/client/clientset/versioned"
)

func NewCmdRestore() *cobra.Command {
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
		Use:               "restore-etcd",
		Short:             "Restores Etcd DB Backup",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags.EnsureRequiredFlags(cmd, "appbinding", "provider", "secret-dir", "data-dir", "initial-cluster", "workload-kind", "workload-name")

			// prepare client

			config, err := clientcmd.BuildConfigFromFlags(masterURL, kubeconfigPath)
			if err != nil {
				return err
			}
			err = license.CheckLicenseEndpoint(config, licenseApiService, SupportedProducts)
			if err != nil {
				return err
			}
			opt.kubeClient, err = kubernetes.NewForConfig(config)
			if err != nil {
				return err
			}
			opt.stashClient, err = stash.NewForConfig(config)
			if err != nil {
				return err
			}
			opt.catalogClient, err = appcatalog_cs.NewForConfig(config)
			if err != nil {
				return err
			}
			opt.config = config

			//Get the restore invoker information
			opt.invoker, err = invoker.ExtractRestoreInvokerInfo(opt.kubeClient, opt.stashClient, opt.invokerKind, opt.invokerName, opt.namespace)
			if err != nil {
				return err
			}

			targetRef := api_v1beta1.TargetRef{
				APIVersion: appcatalog.SchemeGroupVersion.String(),
				Kind:       appcatalog.ResourceKindApp,
				Name:       opt.appBindingName,
			}

			targetStats := api_v1beta1.RestoreMemberStatus{
				Ref: targetRef,
			}

			// Set RestoreCompleted Condition to False
			err = conditions.SetRestoreCompletedConditionToFalse(opt.invoker, targetRef, "Restore process in progress.")
			if err != nil {
				return err
			}

			restoreErr := opt.restoreEtcd()
			if restoreErr != nil {
				targetStats.Phase = api_v1beta1.TargetRestoreFailed
				// Don't return the error. Just log it so that update-status function can update the restore invoker status properly.
				klog.Errorln(restoreErr)
			} else {
				targetStats.Phase = api_v1beta1.TargetRestoreSucceeded
			}
			var restoreOutput = &restic.RestoreOutput{RestoreTargetStatus: targetStats}

			// Set RestoreCompleted Condition to True
			err = conditions.SetRestoreCompletedConditionToTrue(opt.invoker, targetRef, "All steps of restore process has been executed")
			if err != nil {
				return err
			}

			// If output directory specified, then write the output in "output.json" file in the specified directory
			if opt.outputDir != "" {
				return restoreOutput.WriteOutput(filepath.Join(opt.outputDir, restic.DefaultOutputFileName))
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&masterURL, "master", masterURL, "The address of the Kubernetes API server (overrides any value in kubeconfig)")
	cmd.Flags().StringVar(&kubeconfigPath, "kubeconfig", kubeconfigPath, "Path to kubeconfig file with authorization information (the master location is set by the master flag).")
	cmd.Flags().StringVar(&opt.namespace, "namespace", "default", "Namespace of Backup/Restore Session")
	cmd.Flags().StringVar(&opt.appBindingName, "appbinding", opt.appBindingName, "Name of the app binding")

	cmd.Flags().StringVar(&opt.setupOptions.Provider, "provider", opt.setupOptions.Provider, "Backend provider (i.e. gcs, s3, azure etc)")
	cmd.Flags().StringVar(&opt.setupOptions.Bucket, "bucket", opt.setupOptions.Bucket, "Name of the cloud bucket/container (keep empty for local backend)")
	cmd.Flags().StringVar(&opt.setupOptions.Endpoint, "endpoint", opt.setupOptions.Endpoint, "Endpoint for s3/s3 compatible backend or REST backend URL")
	cmd.Flags().StringVar(&opt.setupOptions.Region, "region", opt.setupOptions.Region, "Region for s3/s3 compatible backend")
	cmd.Flags().StringVar(&opt.setupOptions.Path, "path", opt.setupOptions.Path, "Directory inside the bucket where backup will be stored")
	cmd.Flags().StringVar(&opt.setupOptions.SecretDir, "secret-dir", opt.setupOptions.SecretDir, "Directory where storage secret has been mounted")
	cmd.Flags().StringVar(&opt.setupOptions.ScratchDir, "scratch-dir", opt.setupOptions.ScratchDir, "Temporary directory")
	cmd.Flags().BoolVar(&opt.setupOptions.EnableCache, "enable-cache", opt.setupOptions.EnableCache, "Specify whether to enable caching for restic")
	cmd.Flags().Int64Var(&opt.setupOptions.MaxConnections, "max-connections", opt.setupOptions.MaxConnections, "Specify maximum concurrent connections for GCS, Azure and B2 backend")

	cmd.Flags().StringVar(&opt.restoreOptions.Host, "hostname", opt.restoreOptions.Host, "Name of the host machine")
	cmd.Flags().StringVar(&opt.restoreOptions.SourceHost, "source-hostname", opt.restoreOptions.SourceHost, "Name of the host from where data will be restored")
	cmd.Flags().StringSliceVar(&opt.restoreOptions.Snapshots, "snapshots", opt.restoreOptions.Snapshots, "Snapshot to dump")

	cmd.Flags().StringVar(&opt.outputDir, "output-dir", opt.outputDir, "Directory where output.json file will be written (keep empty if you don't need to write output in file)")

	cmd.Flags().StringVar(&opt.etcdArgs, "etcd-args", opt.etcdArgs, "Additional arguments")
	cmd.Flags().StringVar(&opt.etcd.initialCluster, "initial-cluster", opt.etcd.initialCluster, "Initial cluster members name and their urls for the etcd cluster")
	cmd.Flags().StringVar(&opt.etcd.initialClusterToken, "initial-cluster-token", opt.etcd.initialClusterToken, "Initial cluster token for the etcd cluster")
	cmd.Flags().StringVar(&opt.etcd.dataDir, "data-dir", opt.etcd.dataDir, "The etcd data directory")
	cmd.Flags().Int32Var(&opt.waitTimeout, "wait-timeout", opt.waitTimeout, "Time limit to wait for the database to be ready")

	cmd.Flags().StringVar(&opt.workloadKind, "workload-kind", opt.workloadKind, "Type of workload used to deploy etcd DB server")
	cmd.Flags().StringVar(&opt.workloadName, "workload-name", opt.workloadName, "Name of the workload used to deploy etcd DB server")

	cmd.Flags().StringVar(&opt.invokerName, "invoker-name", opt.invokerName, "Name of the restore invoker")
	cmd.Flags().StringVar(&opt.invokerKind, "invoker-kind", opt.invokerKind, "Type of the restore invoker")

	cmd.Flags().StringVar(&opt.addonImage, "image", opt.addonImage, "Stash etcd addon Image")
	cmd.Flags().StringVar(&opt.repositorySecretName, "secret-name", opt.repositorySecretName, "Stash repository secret name")

	return cmd
}

func (opt *options) restoreEtcd() error {

	// get app binding
	appBinding, err := opt.catalogClient.AppcatalogV1alpha1().AppBindings(opt.namespace).Get(context.TODO(), opt.appBindingName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	//Get the endpoint for etcd client from the service mentioned in the appbinding
	opt.etcd.endpoint, err = opt.getEndpoint(appBinding)
	if err != nil {
		return err
	}

	klog.Infoln("Creating RBAC resources for restore interim pods...")
	err = opt.ensureRestorePodRBAC()
	if err != nil {
		return err
	}

	creds, err := opt.getCredential(appBinding)
	if err != nil {
		return err
	}
	//Wait for the DB ready
	klog.Infoln("Waiting for the database to be ready...")
	err = opt.waitForDBReady(creds)
	if err != nil {
		return err
	}

	klog.Infoln("Listing out member pods of etcd cluster")
	// List out the pods used to deploy etcd cluster
	memberPods, err := opt.getEtcdMemberPods()
	if err != nil {
		return err
	}

	// Extract the peer names with urls from initial cluster configuration
	peers := strings.Split(strings.TrimSpace(opt.etcd.initialCluster), ",")

	restorePods := make([]corev1.Pod, 0)
	for i, member := range memberPods {
		peer := strings.SplitN(peers[i], "=", 2)
		name := strings.TrimSpace(peer[0])
		advertisePeerUrl := strings.TrimSpace(peer[1])

		args := make([]string, 0)

		args = append(args, "--download-dir="+filepath.Join(opt.etcd.interimDataDir, EtcdBackupFile))
		args = append(args, "--name="+name)
		args = append(args, "--initial-cluster="+opt.etcd.initialCluster)
		args = append(args, "--initial-cluster-token="+opt.etcd.initialClusterToken)
		args = append(args, "--initial-advertise-peer-urls="+advertisePeerUrl)
		args = append(args, "--data-dir="+filepath.Join(opt.etcd.dataDir, RestoreDirSuffix))

		args = append(args, fmt.Sprintf("--hostname=host-%d", i))

		args = append(args, strings.Fields(opt.etcdArgs)...)

		pod, err := opt.restoreEtcdMember(member, args)
		if err != nil {
			return err
		}
		restorePods = append(restorePods, *pod)
	}

	//Wait until the restore gets applied for every member in the restore dir from the temporary download directory
	klog.Infoln("Waiting for snapshot getting restored...")
	err = opt.waitUntilRestoreComplete(len(memberPods))
	if err != nil {
		return err
	}

	klog.Infoln("Scaling down etcd workload...")
	err = opt.scaleDownWorkload()
	if err != nil {
		return err
	}

	klog.Infoln("Moving restored data into user given data dir...")
	err = opt.replaceOldDataWithRestoredData(restorePods)
	if err != nil {
		return err
	}

	klog.Infoln("Scaling up etcd workload...")
	err = opt.scaleUpWorkload(int32(len(memberPods)))
	if err != nil {
		return err
	}

	// Checking if every initial member is alive after restoring data by qurying at pod-specific endpoint
	for _, peer := range peers {
		val := strings.SplitN(peer, "=", 2)
		url := strings.SplitN(val[1], ":", 3)

		klog.Infoln("Checking if initial member- " + val[0] + " is healthy after data restore...")

		endpoint := url[0] + ":" + url[1] + ":" + strconv.Itoa(int(appBinding.Spec.ClientConfig.Service.Port))

		err = opt.waitForMemberReady(creds, endpoint)
		if err != nil {
			return err
		}
	}

	return nil
}
