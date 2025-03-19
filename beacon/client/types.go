package client

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/rocket-pool/node-manager-core/utils"
)

// Request types
type VoluntaryExitMessage struct {
	Epoch          utils.Uinteger `json:"epoch"`
	ValidatorIndex string         `json:"validator_index"`
}
type VoluntaryExitRequest struct {
	Message   VoluntaryExitMessage `json:"message"`
	Signature utils.ByteArray      `json:"signature"`
}
type BLSToExecutionChangeMessage struct {
	ValidatorIndex     string          `json:"validator_index"`
	FromBLSPubkey      utils.ByteArray `json:"from_bls_pubkey"`
	ToExecutionAddress utils.ByteArray `json:"to_execution_address"`
}
type BLSToExecutionChangeRequest struct {
	Message   BLSToExecutionChangeMessage `json:"message"`
	Signature utils.ByteArray             `json:"signature"`
}

// Response types
type SyncStatusResponse struct {
	Data struct {
		IsSyncing    bool           `json:"is_syncing"`
		HeadSlot     utils.Uinteger `json:"head_slot"`
		SyncDistance utils.Uinteger `json:"sync_distance"`
	} `json:"data"`
}
type Eth2ConfigResponse struct {
	Data struct {
		SecondsPerSlot               utils.Uinteger  `json:"SECONDS_PER_SLOT"`
		SlotsPerEpoch                utils.Uinteger  `json:"SLOTS_PER_EPOCH"`
		EpochsPerSyncCommitteePeriod utils.Uinteger  `json:"EPOCHS_PER_SYNC_COMMITTEE_PERIOD"`
		CapellaForkVersion           utils.ByteArray `json:"CAPELLA_FORK_VERSION"`
	} `json:"data"`
}
type Eth2DepositContractResponse struct {
	Data struct {
		ChainID utils.Uinteger `json:"chain_id"`
		Address common.Address `json:"address"`
	} `json:"data"`
}
type GenesisResponse struct {
	Data struct {
		GenesisTime           utils.Uinteger  `json:"genesis_time"`
		GenesisForkVersion    utils.ByteArray `json:"genesis_fork_version"`
		GenesisValidatorsRoot utils.ByteArray `json:"genesis_validators_root"`
	} `json:"data"`
}
type FinalityCheckpointsResponse struct {
	Data struct {
		PreviousJustified struct {
			Epoch utils.Uinteger `json:"epoch"`
		} `json:"previous_justified"`
		CurrentJustified struct {
			Epoch utils.Uinteger `json:"epoch"`
		} `json:"current_justified"`
		Finalized struct {
			Epoch utils.Uinteger `json:"epoch"`
		} `json:"finalized"`
	} `json:"data"`
}
type ForkResponse struct {
	Data struct {
		PreviousVersion utils.ByteArray `json:"previous_version"`
		CurrentVersion  utils.ByteArray `json:"current_version"`
		Epoch           utils.Uinteger  `json:"epoch"`
	} `json:"data"`
}
type AttestationsResponse struct {
	Data []Attestation `json:"data"`
}
type SignedBeaconBlockHeader struct {
	Message struct {
		Slot          utils.Uinteger  `json:"slot"`
		ProposerIndex string          `json:"proposer_index"`
		ParentRoot    utils.ByteArray `json:"parent_root"`
		StateRoot     utils.ByteArray `json:"state_root"`
		BodyRoot      utils.ByteArray `json:"body_root"`
	} `json:"message"`
	Signature utils.ByteArray `json:"signature"`
}
type ProposerSlashing struct {
	SignedHeader1 SignedBeaconBlockHeader `json:"signed_header_1"`
	SignedHeader2 SignedBeaconBlockHeader `json:"signed_header_2"`
}
type SignedAttestationData struct {
	AttestingIndices []utils.Uinteger `json:"attesting_indices"`
	Data             struct {
		Slot            utils.Uinteger  `json:"slot"`
		Index           utils.Uinteger  `json:"index"`
		BeaconBlockRoot utils.ByteArray `json:"beacon_block_root"`
		Source          struct {
			Epoch utils.Uinteger  `json:"epoch"`
			Root  utils.ByteArray `json:"root"`
		} `json:"source"`
		Target struct {
			Epoch utils.Uinteger  `json:"epoch"`
			Root  utils.ByteArray `json:"root"`
		} `json:"target"`
	} `json:"data"`
	Signature utils.ByteArray `json:"signature"`
}
type AttesterSlashing struct {
	Attestation1 SignedAttestationData `json:"attestation_1"`
	Attestation2 SignedAttestationData `json:"attestation_2"`
}
type BeaconBlockResponse struct {
	Data struct {
		Message struct {
			Slot          utils.Uinteger `json:"slot"`
			ProposerIndex string         `json:"proposer_index"`
			Body          struct {
				Eth1Data struct {
					DepositRoot  utils.ByteArray `json:"deposit_root"`
					DepositCount utils.Uinteger  `json:"deposit_count"`
					BlockHash    utils.ByteArray `json:"block_hash"`
				} `json:"eth1_data"`
				Attestations      []Attestation      `json:"attestations"`
				ProposerSlashings []ProposerSlashing `json:"proposer_slashings"`
				AttesterSlashings []AttesterSlashing `json:"attester_slashings"`
				ExecutionPayload  *struct {
					FeeRecipient utils.ByteArray `json:"fee_recipient"`
					BlockNumber  utils.Uinteger  `json:"block_number"`
				} `json:"execution_payload"`
			} `json:"body"`
		} `json:"message"`
	} `json:"data"`
}
type BeaconBlockHeaderResponse struct {
	Finalized bool `json:"finalized"`
	Data      struct {
		Root      string `json:"root"`
		Canonical bool   `json:"canonical"`
		Header    struct {
			Message struct {
				Slot          utils.Uinteger `json:"slot"`
				ProposerIndex string         `json:"proposer_index"`
			} `json:"message"`
		} `json:"header"`
	} `json:"data"`
}
type ValidatorsResponse struct {
	Data []Validator `json:"data"`
}
type Validator struct {
	Index     string         `json:"index"`
	Balance   utils.Uinteger `json:"balance"`
	Status    string         `json:"status"`
	Validator struct {
		Pubkey                     utils.ByteArray `json:"pubkey"`
		WithdrawalCredentials      utils.ByteArray `json:"withdrawal_credentials"`
		EffectiveBalance           utils.Uinteger  `json:"effective_balance"`
		Slashed                    bool            `json:"slashed"`
		ActivationEligibilityEpoch utils.Uinteger  `json:"activation_eligibility_epoch"`
		ActivationEpoch            utils.Uinteger  `json:"activation_epoch"`
		ExitEpoch                  utils.Uinteger  `json:"exit_epoch"`
		WithdrawableEpoch          utils.Uinteger  `json:"withdrawable_epoch"`
	} `json:"validator"`
}
type SyncDutiesResponse struct {
	Data []SyncDuty `json:"data"`
}
type SyncDuty struct {
	Pubkey               utils.ByteArray  `json:"pubkey"`
	ValidatorIndex       string           `json:"validator_index"`
	SyncCommitteeIndices []utils.Uinteger `json:"validator_sync_committee_indices"`
}
type ProposerDutiesResponse struct {
	Data []ProposerDuty `json:"data"`
}
type ProposerDuty struct {
	ValidatorIndex string `json:"validator_index"`
}

type CommitteesResponse struct {
	Data []Committee `json:"data"`
}

type Attestation struct {
	AggregationBits string `json:"aggregation_bits"`
	Data            struct {
		Slot  utils.Uinteger `json:"slot"`
		Index utils.Uinteger `json:"index"`
	} `json:"data"`
}
