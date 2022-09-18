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
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"gomodules.xyz/flags"
)

var dataDir string

func NewCmdReplaceData() *cobra.Command {
	cmd := &cobra.Command{
		Use:               "replace",
		Short:             "Replace old data with newly restored data.",
		DisableAutoGenTag: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			flags.EnsureRequiredFlags(cmd, "data-dir")

			err := replaceData()
			if err != nil {
				return err
			}

			return nil
		},
	}
	cmd.Flags().StringVar(&dataDir, "data-dir", dataDir, "Directory where to move the restored data")
	return cmd
}

func replaceData() error {
	// Read current files in the data directory
	files, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}

	// Remove the old data from the data directory
	for _, file := range files {
		if file.Name() != RestoreDirSuffix {
			err = os.RemoveAll(filepath.Join(dataDir, file.Name()))
			if err != nil {
				return err
			}
		}
	}

	// Read files in the restore directory
	restoreDir := filepath.Join(dataDir, RestoreDirSuffix)
	files, err = os.ReadDir(restoreDir)
	if err != nil {
		return err
	}

	// Move the restored data from restore directory to data directory by renaming them
	for _, file := range files {
		err = os.Rename(filepath.Join(restoreDir, file.Name()), filepath.Join(dataDir, file.Name()))
		if err != nil {
			return err
		}
	}

	// Remove the empty restore directory
	err = os.RemoveAll(restoreDir)
	if err != nil {
		return err
	}

	return nil
}
