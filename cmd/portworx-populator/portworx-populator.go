package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/kubev2v/forklift/pkg/apis/forklift/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	sourcePvc       string
	sourceNamespace string
	destPvc         string
	destNamespace   string
	secretName      string
	crName          string
	crNamespace     string
	kubeconfig      string
	pvcSize         int64
	ownerUID        string
)

func main() {
	flag.StringVar(&sourcePvc, "source-pvc", "", "Source PVC name (FADA)")
	flag.StringVar(&sourceNamespace, "source-namespace", "", "Source PVC namespace")
	flag.StringVar(&destPvc, "dest-pvc", "", "Destination PVC name (PXD)")
	flag.StringVar(&destNamespace, "dest-namespace", "", "Destination PVC namespace")
	flag.StringVar(&secretName, "secret-name", "", "Secret name with FlashArray credentials")
	flag.StringVar(&crName, "cr-name", "", "PortworxVolumePopulator CR name")
	flag.StringVar(&crNamespace, "cr-namespace", "", "PortworxVolumePopulator CR namespace")
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file")
	flag.Int64Var(&pvcSize, "pvc-size", 0, "Size of PVC in bytes (unused - for compatibility)")
	flag.StringVar(&ownerUID, "owner-uid", "", "Owner UID (unused - for compatibility)")
	klog.InitFlags(nil)
	flag.Parse()

	if sourcePvc == "" || sourceNamespace == "" || destPvc == "" || destNamespace == "" {
		klog.Fatal("source-pvc, source-namespace, dest-pvc, and dest-namespace are required")
	}

	klog.Infof("Starting Portworx populator: %s/%s -> %s/%s", sourceNamespace, sourcePvc, destNamespace, destPvc)

	// Create Kubernetes client
	config, err := getKubeConfig()
	if err != nil {
		klog.Fatalf("Failed to get kubeconfig: %v", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to create clientset: %v", err)
	}

	// Create controller-runtime client for CR updates
	scheme := runtime.NewScheme()
	if err := v1beta1.SchemeBuilder.AddToScheme(scheme); err != nil {
		klog.Fatalf("Failed to add v1beta1 scheme: %v", err)
	}
	k8sClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		klog.Fatalf("Failed to create controller-runtime client: %v", err)
	}

	// Update CR status to Running
	if err := updateCRStatus(k8sClient, "Running", "Waiting for PVCs to be bound", "0%"); err != nil {
		klog.Warningf("Failed to update CR status: %v", err)
	}

	// Wait for source and destination PVCs to be bound
	klog.Info("Waiting for source PVC to be bound...")
	sourcePV, err := waitForPVCBound(clientset, sourceNamespace, sourcePvc)
	if err != nil {
		updateCRStatus(k8sClient, "Failed", fmt.Sprintf("Source PVC not bound: %v", err), "0%")
		klog.Fatalf("Source PVC not bound: %v", err)
	}
	klog.Infof("Source PVC bound to PV: %s", sourcePV)

	klog.Info("Waiting for destination PVC to be bound...")
	destPV, err := waitForPVCBound(clientset, destNamespace, destPvc)
	if err != nil {
		updateCRStatus(k8sClient, "Failed", fmt.Sprintf("Destination PVC not bound: %v", err), "0%")
		klog.Fatalf("Destination PVC not bound: %v", err)
	}
	klog.Infof("Destination PVC bound to PV: %s", destPV)

	// Get FlashArray credentials from environment variables (mounted via envFrom.secretRef)
	faEndpoint, faAPIToken, err := getFlashArrayCredentials()
	if err != nil {
		updateCRStatus(k8sClient, "Failed", fmt.Sprintf("Failed to get FA credentials: %v", err), "0%")
		klog.Fatalf("Failed to get FlashArray credentials: %v", err)
	}

	// Update status to copying
	if err := updateCRStatus(k8sClient, "Running", "Copying data from FADA to PXD", "10%"); err != nil {
		klog.Warningf("Failed to update CR status: %v", err)
	}

	// Run the FADA to PXD migration
	klog.Info("Starting FADA to PXD data copy...")
	if err := runFADAtoPXDCopy(sourcePV, destPV, faEndpoint, faAPIToken); err != nil {
		updateCRStatus(k8sClient, "Failed", fmt.Sprintf("Data copy failed: %v", err), "50%")
		klog.Fatalf("Data copy failed: %v", err)
	}

	// Update status to completed
	if err := updateCRStatus(k8sClient, "Succeeded", "Data copy completed successfully", "100%"); err != nil {
		klog.Warningf("Failed to update CR status: %v", err)
	}

	klog.Info("Portworx populator completed successfully")
}

func getKubeConfig() (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func waitForPVCBound(clientset *kubernetes.Clientset, namespace, pvcName string) (string, error) {
	maxRetries := 60
	for i := 0; i < maxRetries; i++ {
		pvc, err := clientset.CoreV1().PersistentVolumeClaims(namespace).Get(
			context.Background(), pvcName, metav1.GetOptions{})
		if err != nil {
			return "", err
		}

		if pvc.Status.Phase == corev1.ClaimBound {
			return pvc.Spec.VolumeName, nil
		}

		klog.Infof("PVC %s/%s not bound yet (phase: %s), waiting... (%d/%d)",
			namespace, pvcName, pvc.Status.Phase, i+1, maxRetries)
		time.Sleep(5 * time.Second)
	}

	return "", fmt.Errorf("PVC %s/%s did not become bound within timeout", namespace, pvcName)
}

func getFlashArrayCredentials() (string, string, error) {
	// Read credentials from environment variables (mounted via envFrom.secretRef)
	hostname := os.Getenv("STORAGE_HOSTNAME")
	if hostname == "" {
		return "", "", fmt.Errorf("missing STORAGE_HOSTNAME environment variable")
	}

	username := os.Getenv("STORAGE_USERNAME")
	if username == "" {
		return "", "", fmt.Errorf("missing STORAGE_USERNAME environment variable")
	}

	password := os.Getenv("STORAGE_PASSWORD")
	if password == "" {
		return "", "", fmt.Errorf("missing STORAGE_PASSWORD environment variable")
	}

	// Get API token from FlashArray using username/password (via 1.x API)
	apiToken, err := getFlashArrayAPIToken(hostname, username, password)
	if err != nil {
		return "", "", fmt.Errorf("failed to get API token from FlashArray: %w", err)
	}

	return hostname, apiToken, nil
}

// getFlashArrayAPIToken authenticates with FlashArray and returns an API token
// This uses the 1.x API endpoint /api/1.x/auth/apitoken with username/password
func getFlashArrayAPIToken(hostname, username, password string) (string, error) {
	// Skip SSL verification (configurable via STORAGE_SKIP_SSL_VERIFICATION)
	skipSSL := os.Getenv("STORAGE_SKIP_SSL_VERIFICATION") == "true"
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: skipSSL},
		},
	}

	// FlashArray REST API 1.x auth/apitoken endpoint
	// This endpoint accepts username/password and returns an API token
	apiTokenURL := fmt.Sprintf("https://%s/api/1.19/auth/apitoken", hostname)

	// Create request body with username/password
	requestBody := struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}{
		Username: username,
		Password: password,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequest("POST", apiTokenURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return "", fmt.Errorf("failed to create API token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("API token request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("API token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse the API token from response
	var tokenResp struct {
		APIToken string `json:"api_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("failed to parse API token response: %w", err)
	}

	if tokenResp.APIToken == "" {
		return "", fmt.Errorf("empty API token in response")
	}

	klog.Infof("Successfully obtained FlashArray API token")
	return tokenResp.APIToken, nil
}

func runFADAtoPXDCopy(sourcePV, destPV, faEndpoint, faAPIToken string) error {
	klog.Infof("Running FADA to PXD migration: %s -> %s", sourcePV, destPV)

	// Configure the migration
	cfg := DefaultConfig()
	cfg.FAIP = faEndpoint
	cfg.FAAPIToken = faAPIToken
	cfg.FAAPIVer = "2.41" // 2.41+ required for /volumes/diff
	cfg.Jobs = 512
	cfg.Step2Workers = 16
	cfg.StatsInterval = 30

	// Create a logger that integrates with klog
	klogLogger := func(format string, args ...interface{}) {
		klog.Infof(format, args...)
	}

	// Run the migration directly
	klog.Info("Starting FADA to PXD migration...")
	if err := RunMigrationWithLogger(sourcePV, destPV, cfg, klogLogger); err != nil {
		return fmt.Errorf("FADA to PXD migration failed: %w", err)
	}

	klog.Info("FADA to PXD migration completed successfully")
	return nil
}

func updateCRStatus(k8sClient client.Client, phase, message, progress string) error {
	if crName == "" || crNamespace == "" {
		return nil // CR name not provided, skip update
	}

	ctx := context.Background()
	cr := &v1beta1.PortworxVolumePopulator{}
	if err := k8sClient.Get(ctx, client.ObjectKey{
		Name:      crName,
		Namespace: crNamespace,
	}, cr); err != nil {
		return fmt.Errorf("failed to get CR: %w", err)
	}

	cr.Status.Phase = phase
	cr.Status.Message = message
	cr.Status.Progress = progress

	if err := k8sClient.Status().Update(ctx, cr); err != nil {
		return fmt.Errorf("failed to update CR status: %w", err)
	}

	klog.Infof("Updated CR status: phase=%s, progress=%s, message=%s", phase, progress, message)
	return nil
}
