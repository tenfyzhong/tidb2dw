package cdc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	apiv2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/pkg/config"
	putil "github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type CDCConnector struct {
	cdcServer            string
	tables               []string
	startTSO             uint64
	sinkURIConfig        *SinkURIConfig
	SinkURI              *url.URL
	binaryEncodingMethod string
	namespace            string
	changefeedID         string
	columnSelectors      []ColumnSelector
}

func NewCDCConnector(
	cdcHost string, cdcPort int, tables []string, startTSO uint64, storageUri *url.URL,
	flushInterval time.Duration, fileSize int, binaryEncodingMethod string,
) (*CDCConnector, error) {
	return NewCDCConnectorWithOptions(cdcHost, cdcPort, tables, startTSO, storageUri, flushInterval, fileSize, binaryEncodingMethod, CDCConnectorOptions{})
}

func NewCDCConnectorWithOptions(
	cdcHost string, cdcPort int, tables []string, startTSO uint64, storageUri *url.URL,
	flushInterval time.Duration, fileSize int, binaryEncodingMethod string, options CDCConnectorOptions,
) (*CDCConnector, error) {
	sinkURIConfig := &SinkURIConfig{
		storageUri:    storageUri,
		flushInterval: flushInterval,
		fileSize:      fileSize,
		protocol:      "csv",
	}
	sinkURI, err := sinkURIConfig.genSinkURI()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &CDCConnector{
		cdcServer:            fmt.Sprintf("http://%s:%d", cdcHost, cdcPort),
		tables:               tables,
		startTSO:             startTSO,
		sinkURIConfig:        sinkURIConfig,
		SinkURI:              sinkURI,
		binaryEncodingMethod: binaryEncodingMethod,
		namespace:            options.Namespace,
		changefeedID:         options.ChangefeedID,
		columnSelectors:      options.ColumnSelectors,
	}, nil
}

func (c *CDCConnector) CreateChangefeed() error {
	client := &http.Client{}
	replicateCfg := apiv2.GetDefaultReplicaConfig()
	replicateCfg.Sink.CSVConfig.IncludeCommitTs = true
	replicateCfg.Sink.CSVConfig.BinaryEncodingMethod = c.binaryEncodingMethod
	replicateCfg.Sink.CloudStorageConfig = &apiv2.CloudStorageConfig{
		FlushInterval:  putil.AddressOf(c.sinkURIConfig.flushInterval.String()),
		FileSize:       putil.AddressOf(c.sinkURIConfig.fileSize),
		OutputColumnID: putil.AddressOf(true),
	}
	replicateCfg.Sink.DateSeparator = putil.AddressOf(config.DateSeparatorDay.String())
	replicateCfg.Filter = &apiv2.FilterConfig{Rules: c.tables}
	for _, selector := range c.columnSelectors {
		replicateCfg.Sink.ColumnSelectors = append(replicateCfg.Sink.ColumnSelectors, &apiv2.ColumnSelector{
			Matcher: selector.Matcher,
			Columns: selector.Columns,
		})
	}
	cfCfg := &ChangefeedConfig{
		Namespace:     c.namespace,
		ID:            c.changefeedID,
		SinkURI:       c.SinkURI.String(),
		ReplicaConfig: replicateCfg,
	}
	if c.startTSO != 0 {
		cfCfg.StartTs = c.startTSO
	}
	bytesData, _ := json.Marshal(cfCfg)
	url, err := url.JoinPath(c.cdcServer, "api/v2/changefeeds")
	if err != nil {
		return errors.Annotate(err, "join url failed")
	}
	httpReq, _ := http.NewRequest("POST", url, bytes.NewReader(bytesData))
	resp, err := client.Do(httpReq)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("create changefeed failed, status code: %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Trace(err)
	}
	respData := make(map[string]interface{})
	if err = json.Unmarshal(body, &respData); err != nil {
		return errors.Trace(err)
	}
	changefeedID := respData["id"].(string)
	replicateConfig := respData["config"].(map[string]interface{})
	log.Info("create changefeed success", zap.String("changefeed-id", changefeedID), zap.Any("replica-config", replicateConfig))

	return nil
}

func (c *CDCConnector) PauseChangefeed() error {
	return c.lifecycleRequest(http.MethodPost, "pause", nil)
}

func (c *CDCConnector) ResumeChangefeed() error {
	return c.lifecycleRequest(http.MethodPost, "resume", []byte(`{}`))
}

func (c *CDCConnector) DeleteChangefeed() error {
	return c.lifecycleRequest(http.MethodDelete, "", nil)
}

func (c *CDCConnector) lifecycleRequest(method string, action string, body []byte) error {
	if c.changefeedID == "" {
		return errors.New("changefeed id must not be empty")
	}
	parts := []string{"api/v2/changefeeds", c.changefeedID}
	if action != "" {
		parts = append(parts, action)
	}
	reqURL, err := url.JoinPath(c.cdcServer, parts...)
	if err != nil {
		return errors.Annotate(err, "join url failed")
	}
	parsed, err := url.Parse(reqURL)
	if err != nil {
		return errors.Trace(err)
	}
	if c.namespace != "" {
		values := parsed.Query()
		values.Set("namespace", c.namespace)
		parsed.RawQuery = values.Encode()
	}

	httpReq, err := http.NewRequest(method, parsed.String(), bytes.NewReader(body))
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("%s changefeed failed, status code: %d", method, resp.StatusCode)
	}
	return nil
}
