package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/rocket-pool/node-manager-core/beacon"
)

const (
	RequestUrlFormat   = "%s%s"
	RequestContentType = "application/json"

	RequestSyncStatusPath                  = "/eth/v1/node/syncing"
	RequestEth2ConfigPath                  = "/eth/v1/config/spec"
	RequestEth2DepositContractMethod       = "/eth/v1/config/deposit_contract"
	RequestCommitteePath                   = "/eth/v1/beacon/states/%s/committees"
	RequestGenesisPath                     = "/eth/v1/beacon/genesis"
	RequestFinalityCheckpointsPath         = "/eth/v1/beacon/states/%s/finality_checkpoints"
	RequestForkPath                        = "/eth/v1/beacon/states/%s/fork"
	RequestValidatorsPath                  = "/eth/v1/beacon/states/%s/validators"
	RequestVoluntaryExitPath               = "/eth/v1/beacon/pool/voluntary_exits"
	RequestAttestationsPath                = "/eth/v1/beacon/blocks/%s/attestations"
	RequestBeaconBlockPath                 = "/eth/v2/beacon/blocks/%s"
	RequestBeaconBlockHeaderPath           = "/eth/v1/beacon/headers/%s"
	RequestValidatorSyncDuties             = "/eth/v1/validator/duties/sync/%s"
	RequestValidatorProposerDuties         = "/eth/v1/validator/duties/proposer/%s"
	RequestWithdrawalCredentialsChangePath = "/eth/v1/beacon/pool/bls_to_execution_changes"
	EventStreamPath                        = "/eth/v1/events"

	MaxRequestValidatorsCount = 600
)

type BeaconHttpProvider struct {
	providerAddress string
	client          http.Client
}

func NewBeaconHttpProvider(providerAddress string, timeout time.Duration) *BeaconHttpProvider {
	return &BeaconHttpProvider{
		providerAddress: providerAddress,
		client: http.Client{
			Timeout: timeout,
		},
	}
}

func (p *BeaconHttpProvider) Beacon_Attestations(ctx context.Context, blockId string) (AttestationsResponse, bool, error) {
	responseBody, status, err := p.getRequest(ctx, fmt.Sprintf(RequestAttestationsPath, blockId))
	if err != nil {
		return AttestationsResponse{}, false, fmt.Errorf("error getting attestations data for slot %s: %w", blockId, err)
	}
	if status == http.StatusNotFound {
		return AttestationsResponse{}, false, nil
	}
	if status != http.StatusOK {
		return AttestationsResponse{}, false, fmt.Errorf("error getting attestations data for slot %s: HTTP status %d; response body: '%s'", blockId, status, string(responseBody))
	}
	var attestations AttestationsResponse
	if err := json.Unmarshal(responseBody, &attestations); err != nil {
		return AttestationsResponse{}, false, fmt.Errorf("error decoding attestations data for slot %s: %w", blockId, err)
	}
	return attestations, true, nil
}

func (p *BeaconHttpProvider) Beacon_Block(ctx context.Context, blockId string) (BeaconBlockResponse, bool, error) {
	responseBody, status, err := p.getRequest(ctx, fmt.Sprintf(RequestBeaconBlockPath, blockId))
	if err != nil {
		return BeaconBlockResponse{}, false, fmt.Errorf("error getting beacon block data: %w", err)
	}
	if status == http.StatusNotFound {
		return BeaconBlockResponse{}, false, nil
	}
	if status != http.StatusOK {
		return BeaconBlockResponse{}, false, fmt.Errorf("error getting beacon block data: HTTP status %d; response body: '%s'", status, string(responseBody))
	}
	var beaconBlock BeaconBlockResponse
	if err := json.Unmarshal(responseBody, &beaconBlock); err != nil {
		return BeaconBlockResponse{}, false, fmt.Errorf("error decoding beacon block data: %w", err)
	}
	return beaconBlock, true, nil
}

func (p *BeaconHttpProvider) Beacon_Event_Stream(ctx context.Context, topics []string) (chan Event, context.CancelFunc, error) {
	for _, topic := range topics {
		if topic != "head" {
			return nil, nil, fmt.Errorf("unsupported topic: %s", topic)
		}
	}

	// build the url
	query := "?topics=" + strings.Join(topics, ",")
	url := fmt.Sprintf(RequestUrlFormat, p.providerAddress, EventStreamPath) + query

	ch, cancel := startEventStream(ctx, url)
	return ch, cancel, nil
}

func (p *BeaconHttpProvider) Beacon_BlsToExecutionChanges_Post(ctx context.Context, request BLSToExecutionChangeRequest) error {
	requestArray := []BLSToExecutionChangeRequest{request} // This route must be wrapped in an array
	responseBody, status, err := p.postRequest(ctx, RequestWithdrawalCredentialsChangePath, requestArray)
	if err != nil {
		return fmt.Errorf("error broadcasting withdrawal credentials change for validator %s: %w", request.Message.ValidatorIndex, err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("error broadcasting withdrawal credentials change for validator %s: HTTP status %d; response body: '%s'", request.Message.ValidatorIndex, status, string(responseBody))
	}
	return nil
}

func (p *BeaconHttpProvider) Beacon_Committees(ctx context.Context, stateId string, epoch *uint64) (CommitteesResponse, error) {
	var committees CommitteesResponse

	query := ""
	if epoch != nil {
		query = fmt.Sprintf("?epoch=%d", *epoch)
	}

	// Committees responses are large, so let the json decoder read it in a buffered fashion
	clientWithoutTimeout := http.Client{}
	reader, status, err := getRequestReader(ctx, fmt.Sprintf(RequestCommitteePath, stateId)+query, p.providerAddress, clientWithoutTimeout)
	if err != nil {
		return CommitteesResponse{}, fmt.Errorf("error getting committees: %w", err)
	}
	defer func() {
		_ = reader.Close()
	}()

	if status != http.StatusOK {
		body, _ := io.ReadAll(reader)
		return CommitteesResponse{}, fmt.Errorf("error getting committees: HTTP status %d; response body: '%s'", status, string(body))
	}

	d := committeesDecoderPool.Get().(*committeesDecoder)
	defer func() {
		d.currentReader = nil
		committeesDecoderPool.Put(d)
	}()

	d.currentReader = &reader

	// Begin decoding
	if err := d.decoder.Decode(&committees); err != nil {
		return CommitteesResponse{}, fmt.Errorf("error decoding committees: %w", err)
	}

	return committees, nil
}

func (p *BeaconHttpProvider) Beacon_FinalityCheckpoints(ctx context.Context, stateId string) (FinalityCheckpointsResponse, error) {
	responseBody, status, err := p.getRequest(ctx, fmt.Sprintf(RequestFinalityCheckpointsPath, stateId))
	if err != nil {
		return FinalityCheckpointsResponse{}, fmt.Errorf("error getting finality checkpoints: %w", err)
	}
	if status != http.StatusOK {
		return FinalityCheckpointsResponse{}, fmt.Errorf("error getting finality checkpoints: HTTP status %d; response body: '%s'", status, string(responseBody))
	}
	var finalityCheckpoints FinalityCheckpointsResponse
	if err := json.Unmarshal(responseBody, &finalityCheckpoints); err != nil {
		return FinalityCheckpointsResponse{}, fmt.Errorf("error decoding finality checkpoints: %w", err)
	}
	return finalityCheckpoints, nil
}

func (p *BeaconHttpProvider) Beacon_Genesis(ctx context.Context) (GenesisResponse, error) {
	responseBody, status, err := p.getRequest(ctx, RequestGenesisPath)
	if err != nil {
		return GenesisResponse{}, fmt.Errorf("error getting genesis data: %w", err)
	}
	if status != http.StatusOK {
		return GenesisResponse{}, fmt.Errorf("error getting genesis data: HTTP status %d; response body: '%s'", status, string(responseBody))
	}
	var genesis GenesisResponse
	if err := json.Unmarshal(responseBody, &genesis); err != nil {
		return GenesisResponse{}, fmt.Errorf("error decoding genesis: %w", err)
	}
	return genesis, nil
}

func (p *BeaconHttpProvider) Beacon_Header(ctx context.Context, blockId string) (BeaconBlockHeaderResponse, bool, error) {
	responseBody, status, err := p.getRequest(ctx, fmt.Sprintf(RequestBeaconBlockHeaderPath, blockId))
	if err != nil {
		return BeaconBlockHeaderResponse{}, false, fmt.Errorf("error getting beacon block header data: %w", err)
	}
	if status == http.StatusNotFound {
		return BeaconBlockHeaderResponse{}, false, nil
	}
	if status != http.StatusOK {
		return BeaconBlockHeaderResponse{}, false, fmt.Errorf("error getting beacon block header data: HTTP status %d; response body: '%s'", status, string(responseBody))
	}
	var beaconBlock BeaconBlockHeaderResponse
	if err := json.Unmarshal(responseBody, &beaconBlock); err != nil {
		return BeaconBlockHeaderResponse{}, false, fmt.Errorf("error getting beacon block header data: %w", err)
	}
	return beaconBlock, true, nil
}

func (p *BeaconHttpProvider) Beacon_Validators(ctx context.Context, stateId string, ids []string) (ValidatorsResponse, error) {
	var query string
	if len(ids) > 0 {
		query = fmt.Sprintf("?id=%s", strings.Join(ids, ","))
	}
	responseBody, status, err := p.getRequestWithoutTimeout(ctx, fmt.Sprintf(RequestValidatorsPath, stateId)+query)
	if err != nil {
		return ValidatorsResponse{}, fmt.Errorf("error getting validators: %w", err)
	}
	if status != http.StatusOK {
		return ValidatorsResponse{}, fmt.Errorf("error getting validators: HTTP status %d; response body: '%s'", status, string(responseBody))
	}
	var validators ValidatorsResponse
	if err := json.Unmarshal(responseBody, &validators); err != nil {
		return ValidatorsResponse{}, fmt.Errorf("error decoding validators: %w", err)
	}
	return validators, nil
}

func (p *BeaconHttpProvider) Beacon_VoluntaryExits_Post(ctx context.Context, request VoluntaryExitRequest) error {
	responseBody, status, err := p.postRequest(ctx, RequestVoluntaryExitPath, request)
	if err != nil {
		return fmt.Errorf("error broadcasting exit for validator at index %s: %w", request.Message.ValidatorIndex, err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("error broadcasting exit for validator at index %s: HTTP status %d; response body: '%s'", request.Message.ValidatorIndex, status, string(responseBody))
	}
	return nil
}

func (p *BeaconHttpProvider) Config_DepositContract(ctx context.Context) (Eth2DepositContractResponse, error) {
	responseBody, status, err := p.getRequest(ctx, RequestEth2DepositContractMethod)
	if err != nil {
		return Eth2DepositContractResponse{}, fmt.Errorf("error getting eth2 deposit contract: %w", err)
	}
	if status != http.StatusOK {
		return Eth2DepositContractResponse{}, fmt.Errorf("error gettingeth2 deposit contract: HTTP status %d; response body: '%s'", status, string(responseBody))
	}
	var eth2DepositContract Eth2DepositContractResponse
	if err := json.Unmarshal(responseBody, &eth2DepositContract); err != nil {
		return Eth2DepositContractResponse{}, fmt.Errorf("error decoding eth2 deposit contract: %w", err)
	}
	return eth2DepositContract, nil
}

func (p *BeaconHttpProvider) Config_Spec(ctx context.Context) (Eth2ConfigResponse, error) {
	responseBody, status, err := p.getRequest(ctx, RequestEth2ConfigPath)
	if err != nil {
		return Eth2ConfigResponse{}, fmt.Errorf("error getting eth2 config: %w", err)
	}
	if status != http.StatusOK {
		return Eth2ConfigResponse{}, fmt.Errorf("error getting eth2 config: HTTP status %d; response body: '%s'", status, string(responseBody))
	}
	var eth2Config Eth2ConfigResponse
	if err := json.Unmarshal(responseBody, &eth2Config); err != nil {
		return Eth2ConfigResponse{}, fmt.Errorf("error decoding eth2 config: %w", err)
	}
	return eth2Config, nil
}

func (p *BeaconHttpProvider) Node_Syncing(ctx context.Context) (SyncStatusResponse, error) {
	responseBody, status, err := p.getRequest(ctx, RequestSyncStatusPath)
	if err != nil {
		return SyncStatusResponse{}, fmt.Errorf("error getting node sync status: %w", err)
	}
	if status != http.StatusOK {
		return SyncStatusResponse{}, fmt.Errorf("error getting node sync status: HTTP status %d; response body: '%s'", status, string(responseBody))
	}
	var syncStatus SyncStatusResponse
	if err := json.Unmarshal(responseBody, &syncStatus); err != nil {
		return SyncStatusResponse{}, fmt.Errorf("error decoding node sync status: %w", err)
	}
	return syncStatus, nil
}

func (p *BeaconHttpProvider) Validator_DutiesProposer(ctx context.Context, indices []string, epoch uint64) (ProposerDutiesResponse, error) {
	responseBody, status, err := p.getRequest(ctx, fmt.Sprintf(RequestValidatorProposerDuties, strconv.FormatUint(epoch, 10)))
	if err != nil {
		return ProposerDutiesResponse{}, fmt.Errorf("error getting validator proposer duties: %w", err)
	}
	if status != http.StatusOK {
		return ProposerDutiesResponse{}, fmt.Errorf("error getting validator proposer duties: HTTP status %d; response body: '%s'", status, string(responseBody))
	}

	var syncDuties ProposerDutiesResponse
	if err := json.Unmarshal(responseBody, &syncDuties); err != nil {
		return ProposerDutiesResponse{}, fmt.Errorf("error decoding validator proposer duties data: %w", err)
	}
	return syncDuties, nil
}

func (p *BeaconHttpProvider) Validator_DutiesSync_Post(ctx context.Context, indices []string, epoch uint64) (SyncDutiesResponse, error) {
	// Perform the post request
	responseBody, status, err := p.postRequest(ctx, fmt.Sprintf(RequestValidatorSyncDuties, strconv.FormatUint(epoch, 10)), indices)

	if err != nil {
		return SyncDutiesResponse{}, fmt.Errorf("error getting validator sync duties: %w", err)
	}
	if status != http.StatusOK {
		return SyncDutiesResponse{}, fmt.Errorf("error getting validator sync duties: HTTP status %d; response body: '%s'", status, string(responseBody))
	}

	var syncDuties SyncDutiesResponse
	if err := json.Unmarshal(responseBody, &syncDuties); err != nil {
		return SyncDutiesResponse{}, fmt.Errorf("error decoding validator sync duties data: %w", err)
	}
	return syncDuties, nil
}

// ==========================
// === Internal Functions ===
// ==========================

// Make a GET request to the beacon node and read the body of the response
func (p *BeaconHttpProvider) getRequest(ctx context.Context, requestPath string) ([]byte, int, error) {
	return getRequestImpl(ctx, requestPath, p.providerAddress, p.client)
}

// Make a GET request to the beacon node and read the body of the response
func (p *BeaconHttpProvider) getRequestWithoutTimeout(ctx context.Context, requestPath string) ([]byte, int, error) {
	clientWithoutTimeout := http.Client{}
	return getRequestImpl(ctx, requestPath, p.providerAddress, clientWithoutTimeout)
}

// Make a GET request to the beacon node and read the body of the response
func getRequestImpl(ctx context.Context, requestPath string, providerAddress string, client http.Client) ([]byte, int, error) {
	// Send request
	reader, status, err := getRequestReader(ctx, requestPath, providerAddress, client)
	if err != nil {
		return []byte{}, 0, err
	}
	defer func() {
		_ = reader.Close()
	}()

	// Get response
	body, err := io.ReadAll(reader)
	if err != nil {
		return []byte{}, 0, err
	}

	// Return
	return body, status, nil
}

// Make a POST request to the beacon node
func (p *BeaconHttpProvider) postRequest(ctx context.Context, requestPath string, requestBody any) ([]byte, int, error) {
	// Get request body
	requestBodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return []byte{}, 0, err
	}
	requestBodyReader := bytes.NewReader(requestBodyBytes)

	// Create the request
	path := fmt.Sprintf(RequestUrlFormat, p.providerAddress, requestPath)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, path, requestBodyReader)
	if err != nil {
		return nil, 0, fmt.Errorf("error creating POST request to [%s]: %w", path, err)
	}
	request.Header.Set("Content-Type", RequestContentType)

	// Submit the request
	response, err := p.client.Do(request)
	if err != nil {
		return []byte{}, 0, fmt.Errorf("error running POST request to [%s]: %w", path, err)
	}
	defer func() {
		_ = response.Body.Close()
	}()

	// Get response
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return []byte{}, 0, err
	}

	// Return
	return body, response.StatusCode, nil
}

// Get an eth2 epoch number by time
func epochAt(config beacon.Eth2Config, time uint64) uint64 {
	return config.GenesisEpoch + (time-config.GenesisTime)/config.SecondsPerEpoch
}

// Make a GET request but do not read its body yet (allows buffered decoding)
func getRequestReader(ctx context.Context, requestPath string, providerAddress string, client http.Client) (io.ReadCloser, int, error) {
	// Make the request
	path := fmt.Sprintf(RequestUrlFormat, providerAddress, requestPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("error creating GET request to [%s]: %w", path, err)
	}
	req.Header.Set("Content-Type", RequestContentType)

	// Submit the request
	response, err := client.Do(req)
	if err != nil {
		// Remove the query for readability
		trimmedPath, _, _ := strings.Cut(path, "?")
		return nil, 0, fmt.Errorf("error running GET request to [%s]: %w", trimmedPath, err)
	}
	return response.Body, response.StatusCode, nil
}

// ==========================
// === Committees Decoder ===
// ==========================

type committeesDecoder struct {
	decoder       *json.Decoder
	currentReader *io.ReadCloser
}

// Read will be called by the json decoder to request more bytes of data from
// the beacon node's committees response. Since the decoder is reused, we
// need to avoid sending it io.EOF, or it will enter an unusable state and can
// not be reused later.
//
// On subsequent calls to Decode, the decoder resets its internal buffer, which
// means any data it reads between the last json token and EOF is correctly
// discarded.
func (c *committeesDecoder) Read(p []byte) (int, error) {
	n, err := (*c.currentReader).Read(p)
	if err == io.EOF {
		return n, nil
	}

	return n, err
}

var committeesDecoderPool sync.Pool = sync.Pool{
	New: func() any {
		var out committeesDecoder

		out.decoder = json.NewDecoder(&out)
		return &out
	},
}
