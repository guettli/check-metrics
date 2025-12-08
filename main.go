package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

const (
	defaultIntervalSeconds = 30
	metricsContainerPort   = 8080
)

var errScrapeRunning = errors.New("scrape already running")

func main() {
	root := &cobra.Command{
		Use:           "check-metrics",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newScrapeCommand())
	root.AddCommand(newPerSecondCommand())
	if err := root.Execute(); err != nil {
		if errors.Is(err, errScrapeRunning) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(100)
		}
		log.Fatalf("command failed: %v", err)
	}
}

func newScrapeCommand() *cobra.Command {
	var configPath string
	cmd := &cobra.Command{
		Use:   "scrape",
		Short: "scrape pod metrics periodically",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			path, err := resolveConfigPath(configPath)
			if err != nil {
				return err
			}
			return runScrape(ctx, path)
		},
	}
	cmd.Flags().StringVar(&configPath, "config", "", "path to configuration YAML (searches upward from current directory)")
	return cmd
}

func newPerSecondCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "per-second counter-name",
		Short: "compute per-second rate for a counter using cached snapshots",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			return runPerSecond(ctx, args[0])
		},
	}
	return cmd
}

func resolveConfigPath(fallback string) (string, error) {
	if fallback != "" {
		return fallback, nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		candidate := filepath.Join(dir, ".config", "check-metrics", "check-metrics.yaml")
		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() {
			return candidate, nil
		}
		if parent := filepath.Dir(dir); parent == dir {
			break
		}
	}
	return "", fmt.Errorf("config not found; searched from %s upward", wd)
}

func runScrape(ctx context.Context, configPath string) error {
	cfg, err := loadConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if len(cfg.Pods) == 0 {
		return fmt.Errorf("configuration must define at least one pod matcher")
	}

	interval := time.Duration(cfg.DefaultScrapeIntervalSeconds) * time.Second
	if interval <= 0 {
		interval = defaultIntervalSeconds * time.Second
	}

	clientset, restCfg, clusterName, err := buildKubeClient()
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("find HOME: %w", err)
	}

	cacheBase := filepath.Join(home, ".cache", "check-metrics")
	cacheDir := filepath.Join(cacheBase, clusterName)
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return fmt.Errorf("create cache dir: %w", err)
	}

	lock, err := acquireClusterLock(cacheDir)
	if err != nil {
		return err
	}
	defer lock.Unlock()

	log.Printf("scraping cluster %q every %s", clusterName, interval)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		listCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		targets, err := discoverTargets(listCtx, clientset, cfg.Pods)
		cancel()
		if err != nil {
			log.Printf("discover targets: %v", err)
		}

		if len(targets) == 0 {
			log.Print("no matching pods found")
		}

		for _, target := range targets {
			reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := scrapeAndStore(reqCtx, clientset, restCfg, target, clusterName, cacheBase); err != nil {
				log.Printf("scrape %s/%s: %v", target.Namespace, target.Name, err)
			}
			cancel()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func runPerSecond(ctx context.Context, counterName string) error {
	_, _, clusterName, err := buildKubeClient()
	if err != nil {
		return fmt.Errorf("kubernetes client: %w", err)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("find HOME: %w", err)
	}

	cacheDir := filepath.Join(home, ".cache", "check-metrics", clusterName)
	stats, zeroCount, window, err := computeRates(cacheDir, counterName)
	if err != nil {
		return err
	}
	if len(stats) == 0 {
		fmt.Printf("no data for %s\n", counterName)
		if zeroCount > 0 {
			fmt.Printf("%d counters had zero delta\n", zeroCount)
		}
		return nil
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].rate > stats[j].rate
	})
	header := fmt.Sprintf("rates for %s", counterName)
	if window > 0 {
		header = fmt.Sprintf("%s (window %.0fs)", header, window)
	}
	fmt.Println(header)
	for _, stat := range stats {
		fmt.Printf("%s -> %.6f/s (delta=%.0f)\n", stat.key, stat.rate, stat.delta)
	}
	if zeroCount > 0 {
		fmt.Printf("%d counters had zero delta\n", zeroCount)
	}
	return nil
}

type rateStat struct {
	key      string
	delta    float64
	duration float64
	rate     float64
}

const snapshotTimeFormat = "20060102T150405.000000000Z"

func computeRates(cacheDir, counterName string) ([]rateStat, int, float64, error) {
	nsEntries, err := os.ReadDir(cacheDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, 0, nil
		}
		return nil, 0, 0, fmt.Errorf("read cache dir: %w", err)
	}

	totalDelta := make(map[string]float64)
	totalDuration := make(map[string]float64)
	window := 0.0

	for _, ns := range nsEntries {
		if !ns.IsDir() {
			continue
		}
		nsPath := filepath.Join(cacheDir, ns.Name())
		podEntries, err := os.ReadDir(nsPath)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("read namespace dir: %w", err)
		}
		for _, pod := range podEntries {
			if !pod.IsDir() {
				continue
			}
			podPath := filepath.Join(nsPath, pod.Name())
			prev, latest, ok, err := findLatestPair(podPath)
			if err != nil {
				return nil, 0, 0, err
			}
			if !ok {
				continue
			}

			latestData, err := os.ReadFile(latest.path)
			if err != nil {
				return nil, 0, 0, fmt.Errorf("read %s: %w", latest.path, err)
			}
			prevData, err := os.ReadFile(prev.path)
			if err != nil {
				return nil, 0, 0, fmt.Errorf("read %s: %w", prev.path, err)
			}
			latestMap := parseCounter(latestData, counterName)
			prevMap := parseCounter(prevData, counterName)
			deltaSeconds := latest.ts.Sub(prev.ts).Seconds()
			if deltaSeconds <= 0 {
				continue
			}
			if window == 0 {
				window = deltaSeconds
			}
			for key, latestVal := range latestMap {
				prevVal := prevMap[key]
				delta := latestVal - prevVal
				if delta < 0 {
					continue
				}
				totalDelta[key] += delta
				totalDuration[key] += deltaSeconds
			}
		}
	}

	var stats []rateStat
	zeroCount := 0
	for key, delta := range totalDelta {
		duration := totalDuration[key]
		if duration <= 0 {
			continue
		}
		if delta == 0 {
			zeroCount++
			continue
		}
		stats = append(stats, rateStat{
			key:      key,
			delta:    delta,
			duration: duration,
			rate:     delta / duration,
		})
	}
	return stats, zeroCount, window, nil
}

type snapshot struct {
	path string
	ts   time.Time
}

func findLatestPair(dir string) (prev, latest snapshot, ok bool, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return snapshot{}, snapshot{}, false, fmt.Errorf("read pod dir: %w", err)
	}
	var snaps []snapshot
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".prom") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".prom")
		ts, err := time.Parse(snapshotTimeFormat, name)
		if err != nil {
			continue
		}
		snaps = append(snaps, snapshot{
			path: filepath.Join(dir, entry.Name()),
			ts:   ts,
		})
	}
	if len(snaps) < 2 {
		return snapshot{}, snapshot{}, false, nil
	}
	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].ts.Before(snaps[j].ts)
	})
	return snaps[len(snaps)-2], snaps[len(snaps)-1], true, nil
}

func parseCounter(data []byte, counterName string) map[string]float64 {
	values := map[string]float64{}
	lines := bytes.Split(data, []byte("\n"))
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		if !bytes.HasPrefix(line, []byte(counterName)) {
			continue
		}
		line = line[len(counterName):]
		var labels []byte
		if len(line) > 0 && line[0] == '{' {
			end := bytes.IndexByte(line, '}')
			if end == -1 {
				continue
			}
			labels = line[1:end]
			line = line[end+1:]
		}
		line = bytes.TrimSpace(line)
		val, err := strconv.ParseFloat(string(line), 64)
		if err != nil {
			continue
		}
		key := buildSeriesKey(counterName, string(labels))
		values[key] += val
	}
	return values
}

func canonicalLabelString(raw string) string {
	if raw == "" {
		return ""
	}
	pairs := bytes.Split([]byte(raw), []byte(","))
	kvs := make([]string, 0, len(pairs))
	for _, pair := range pairs {
		pair = bytes.TrimSpace(pair)
		if len(pair) == 0 {
			continue
		}
		kvs = append(kvs, string(pair))
	}
	sort.Strings(kvs)
	return strings.Join(kvs, ",")
}

func buildSeriesKey(counterName, rawLabels string) string {
	canonical := canonicalLabelString(rawLabels)
	if canonical == "" {
		return counterName
	}
	return fmt.Sprintf("%s{%s}", counterName, canonical)
}

func loadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func buildKubeClient() (*kubernetes.Clientset, *rest.Config, string, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{}
	kubeConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	restCfg, err := kubeConfig.ClientConfig()
	if err != nil {
		return nil, nil, "", err
	}

	rawConfig, err := kubeConfig.RawConfig()
	if err != nil {
		return nil, nil, "", err
	}

	clusterName := rawConfig.CurrentContext
	if ctx, ok := rawConfig.Contexts[rawConfig.CurrentContext]; ok {
		clusterName = ctx.Cluster
	}
	if clusterName == "" {
		clusterName = "default"
	}

	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, nil, "", err
	}
	return clientset, restCfg, clusterName, nil
}

func discoverTargets(ctx context.Context, clientset *kubernetes.Clientset, matchers []PodMatcher) ([]PodTarget, error) {
	seen := make(map[string]struct{})
	var targets []PodTarget

	for _, matcher := range matchers {
		pattern := strings.TrimSpace(matcher.Name)
		if pattern == "" {
			continue
		}

		listNamespace := matcher.Namespace
		if listNamespace == "" {
			listNamespace = metav1.NamespaceAll
		}

		podList, err := clientset.CoreV1().Pods(listNamespace).List(ctx, metav1.ListOptions{})
		if err != nil {
			return nil, err
		}

		for _, pod := range podList.Items {
			if pod.Status.Phase != corev1.PodRunning {
				continue
			}
			matched, err := path.Match(pattern, pod.Name)
			if err != nil {
				return nil, err
			}
			if !matched {
				continue
			}

			key := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}

			port := matcher.Port
			if port <= 0 {
				if inferred, ok := findMetricsPort(pod); ok {
					port = inferred
				} else {
					port = metricsContainerPort
				}
			}
			targets = append(targets, PodTarget{
				Name:      pod.Name,
				Namespace: pod.Namespace,
				Port:      port,
			})
		}
	}
	return targets, nil
}

func findMetricsPort(pod corev1.Pod) (int, bool) {
	for _, container := range pod.Spec.Containers {
		for _, cp := range container.Ports {
			if cp.ContainerPort <= 0 {
				continue
			}
			name := strings.ToLower(cp.Name)
			if name == "" {
				continue
			}
			if strings.Contains(name, "metrics") {
				return int(cp.ContainerPort), true
			}
		}
	}
	return 0, false
}

func scrapeAndStore(ctx context.Context, clientset *kubernetes.Clientset, restCfg *rest.Config, target PodTarget, clusterName, cacheBase string) error {
	data, err := scrapeViaPortForward(ctx, clientset, restCfg, target)
	if err != nil {
		return err
	}

	dir := filepath.Join(cacheBase, clusterName, target.Namespace, target.Name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	timestamp := time.Now().UTC().Format("20060102T150405.000000000Z")
	file := filepath.Join(dir, fmt.Sprintf("%s.prom", timestamp))
	if err := os.WriteFile(file, data, 0o644); err != nil {
		return err
	}
	log.Printf("stored metrics for %s/%s on port %d", target.Namespace, target.Name, target.Port)
	return nil
}

func scrapeViaPortForward(ctx context.Context, clientset *kubernetes.Clientset, restCfg *rest.Config, target PodTarget) ([]byte, error) {
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(target.Namespace).
		Name(target.Name).
		SubResource("portforward")

	cfg := rest.CopyConfig(restCfg)
	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		return nil, err
	}

	stopCh := make(chan struct{}, 1)
	readyCh := make(chan struct{})
	errCh := make(chan error, 1)

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, http.MethodPost, req.URL())
	forwarder, err := portforward.New(dialer, []string{fmt.Sprintf("0:%d", target.Port)}, stopCh, readyCh, io.Discard, io.Discard)
	if err != nil {
		return nil, err
	}

	go func() {
		errCh <- forwarder.ForwardPorts()
	}()

	select {
	case <-readyCh:
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		close(stopCh)
		return nil, ctx.Err()
	}

	defer func() {
		close(stopCh)
		<-errCh
	}()

	ports, err := forwarder.GetPorts()
	if err != nil {
		return nil, err
	}
	if len(ports) == 0 {
		return nil, fmt.Errorf("port-forward did not report a local port")
	}
	localPort := int(ports[0].Local)
	if localPort <= 0 {
		return nil, fmt.Errorf("port-forward returned invalid local port %d", localPort)
	}

	reqHTTP, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/metrics", localPort), nil)
	if err != nil {
		return nil, err
	}

	res, err := http.DefaultClient.Do(reqHTTP)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("local metrics request returned %s", res.Status)
	}

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("empty payload")
	}
	return data, nil
}

type Config struct {
	DefaultScrapeIntervalSeconds int          `yaml:"defaultScrapeIntervalSeconds"`
	Pods                         []PodMatcher `yaml:"pods"`
}

type PodMatcher struct {
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
	Port      int    `yaml:"port"`
}

type PodTarget struct {
	Name      string
	Namespace string
	Port      int
}

func acquireClusterLock(cacheDir string) (*flock.Flock, error) {
	lockPath := filepath.Join(cacheDir, "scrape.lock")
	lock := flock.New(lockPath)
	locked, err := lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("acquire lock: %w", err)
	}
	if !locked {
		return nil, fmt.Errorf("%w (lock=%s)", errScrapeRunning, lockPath)
	}
	return lock, nil
}
