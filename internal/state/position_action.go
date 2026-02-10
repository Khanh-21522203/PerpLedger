package state

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ActionType defines the type of position action.
// Per doc §13: unified framework for liquidation, ADL, and risk-based deleverage.
type ActionType int32

const (
	ActionTypeLiquidation ActionType = iota
	ActionTypeADL                    // Auto-Deleverage
	ActionTypeDeleverage             // Risk-based deleverage
)

func (at ActionType) String() string {
	switch at {
	case ActionTypeLiquidation:
		return "Liquidation"
	case ActionTypeADL:
		return "ADL"
	case ActionTypeDeleverage:
		return "Deleverage"
	default:
		return "Unknown"
	}
}

// ActionState represents the state of a position action.
// Per doc §13: Normal → Triggered → Executing → PartialFill → Completed/Deficit/Escalated/Cancelled
type ActionState int32

const (
	ActionStateNormal     ActionState = iota
	ActionStateTriggered              // Signal detected, action queued
	ActionStateExecuting              // First fill received
	ActionStatePartialFill            // Some fills received, more expected
	ActionStateCompleted              // Fully executed
	ActionStateDeficit                // Completed with deficit (bankruptcy)
	ActionStateEscalated              // Escalated to next action type (e.g. Liquidation → ADL)
	ActionStateCancelled              // Cancelled (e.g. margin recovered)
)

func (as ActionState) String() string {
	switch as {
	case ActionStateNormal:
		return "Normal"
	case ActionStateTriggered:
		return "Triggered"
	case ActionStateExecuting:
		return "Executing"
	case ActionStatePartialFill:
		return "PartialFill"
	case ActionStateCompleted:
		return "Completed"
	case ActionStateDeficit:
		return "Deficit"
	case ActionStateEscalated:
		return "Escalated"
	case ActionStateCancelled:
		return "Cancelled"
	default:
		return "Unknown"
	}
}

// CanTransitionTo validates action state transitions per doc §13.
func (as ActionState) CanTransitionTo(next ActionState) bool {
	transitions := map[ActionState][]ActionState{
		ActionStateNormal: {
			ActionStateTriggered,
		},
		ActionStateTriggered: {
			ActionStateExecuting,
			ActionStateCancelled, // Margin recovered before first fill
		},
		ActionStateExecuting: {
			ActionStatePartialFill,
			ActionStateCompleted,
			ActionStateDeficit,
		},
		ActionStatePartialFill: {
			ActionStatePartialFill, // More partial fills
			ActionStateCompleted,
			ActionStateDeficit,
			ActionStateCancelled, // Margin recovered
		},
		ActionStateCompleted: {
			// Terminal state
		},
		ActionStateDeficit: {
			ActionStateEscalated, // Escalate to next action type
		},
		ActionStateEscalated: {
			// Terminal — new action created for escalation
		},
		ActionStateCancelled: {
			// Terminal state
		},
	}

	allowed, ok := transitions[as]
	if !ok {
		return false
	}
	for _, a := range allowed {
		if next == a {
			return true
		}
	}
	return false
}

// PositionAction is an in-memory struct tracking a signal-driven position action.
// Per doc §13: this is NOT a database entity — it lives only in the deterministic core.
type PositionAction struct {
	ActionID      uuid.UUID
	ActionType    ActionType
	UserID        uuid.UUID
	MarketID      string
	State         ActionState
	TriggeredAt   int64     // Sequence when triggered
	TriggeredTime time.Time // Timestamp when triggered
	InitialSize   int64     // Position size at trigger time
	FilledSize    int64     // Total filled so far
	RemainingSize int64     // InitialSize - FilledSize
	Deficit       int64     // Negative equity after full close (bankruptcy)
	Priority      int       // Lower = higher priority
	ParentID      *uuid.UUID // If escalated from another action
}

// IsTerminal returns true if the action is in a terminal state.
func (pa *PositionAction) IsTerminal() bool {
	return pa.State == ActionStateCompleted ||
		pa.State == ActionStateDeficit ||
		pa.State == ActionStateEscalated ||
		pa.State == ActionStateCancelled
}

// SignalHandler evaluates whether a position action should be triggered.
// Per doc §13: each ActionType has its own SignalHandler implementation.
type SignalHandler interface {
	// Evaluate checks if the signal condition is met for a position.
	// Returns true if the action should be triggered.
	Evaluate(pos *Position, marginFraction int64, params *RiskParams) bool

	// ActionType returns the type of action this handler triggers.
	ActionType() ActionType

	// Priority returns the priority of this handler (lower = higher priority).
	Priority() int
}

// LiquidationSignalHandler triggers liquidation when margin < MM.
type LiquidationSignalHandler struct{}

func (h *LiquidationSignalHandler) Evaluate(pos *Position, marginFraction int64, params *RiskParams) bool {
	return marginFraction < params.MMFraction
}

func (h *LiquidationSignalHandler) ActionType() ActionType {
	return ActionTypeLiquidation
}

func (h *LiquidationSignalHandler) Priority() int {
	return 0 // Highest priority
}

// PositionActionManager manages active position actions.
type PositionActionManager struct {
	actions        map[uuid.UUID]*PositionAction // actionID -> action
	signalHandlers []SignalHandler
	positionMgr    *PositionManager
}

func NewPositionActionManager(pm *PositionManager) *PositionActionManager {
	return &PositionActionManager{
		actions:     make(map[uuid.UUID]*PositionAction),
		positionMgr: pm,
		signalHandlers: []SignalHandler{
			&LiquidationSignalHandler{},
			// Future: &ADLSignalHandler{}, &DeleverageSignalHandler{}
		},
	}
}

// EvaluateSignals checks all positions in a market for triggered signals.
// Per doc §13: signals are evaluated after every mark price update.
func (pam *PositionActionManager) EvaluateSignals(
	marketID string,
	marginFractions map[uuid.UUID]int64, // userID -> margin fraction
	riskParams *RiskParams,
) []*PositionAction {
	var triggered []*PositionAction

	allPositions := pam.positionMgr.GetAllPositions()

	for _, pos := range allPositions {
		if pos.MarketID != marketID || pos.IsFlat() {
			continue
		}

		// Skip if already has active action
		if pam.hasActiveAction(pos.UserID, pos.MarketID) {
			continue
		}

		marginFraction, ok := marginFractions[pos.UserID]
		if !ok {
			continue
		}

		// Check each signal handler in priority order
		for _, handler := range pam.signalHandlers {
			if handler.Evaluate(pos, marginFraction, riskParams) {
				action := pam.triggerAction(handler, pos)
				triggered = append(triggered, action)
				break // Only one action per position at a time
			}
		}
	}

	return triggered
}

func (pam *PositionActionManager) triggerAction(handler SignalHandler, pos *Position) *PositionAction {
	action := &PositionAction{
		ActionID:      uuid.New(),
		ActionType:    handler.ActionType(),
		UserID:        pos.UserID,
		MarketID:      pos.MarketID,
		State:         ActionStateTriggered,
		InitialSize:   pos.Size,
		RemainingSize: pos.Size,
		Priority:      handler.Priority(),
	}

	pam.actions[action.ActionID] = action
	return action
}

// ProcessFill handles a fill for an active action.
// Per doc §13: shared fill processor across all action types.
func (pam *PositionActionManager) ProcessFill(
	actionID uuid.UUID,
	fillQuantity int64,
) error {
	action, ok := pam.actions[actionID]
	if !ok {
		return fmt.Errorf("unknown action_id: %s", actionID)
	}

	if action.IsTerminal() {
		return fmt.Errorf("action %s is in terminal state %s", actionID, action.State)
	}

	// Transition to Executing on first fill
	if action.State == ActionStateTriggered {
		if !action.State.CanTransitionTo(ActionStateExecuting) {
			return fmt.Errorf("invalid transition: %s → Executing", action.State)
		}
		action.State = ActionStateExecuting
	}

	action.FilledSize += fillQuantity
	action.RemainingSize -= fillQuantity

	if action.RemainingSize < 0 {
		return fmt.Errorf("action overfilled")
	}

	if action.RemainingSize == 0 {
		action.State = ActionStateCompleted
	} else {
		action.State = ActionStatePartialFill
	}

	return nil
}

// CancelAction cancels an action (e.g. margin recovered).
func (pam *PositionActionManager) CancelAction(actionID uuid.UUID) error {
	action, ok := pam.actions[actionID]
	if !ok {
		return nil // Already removed
	}

	if !action.State.CanTransitionTo(ActionStateCancelled) {
		return fmt.Errorf("cannot cancel action in state %s", action.State)
	}

	action.State = ActionStateCancelled
	return nil
}

// EscalateAction escalates an action to the next type (e.g. Liquidation → ADL).
// Per doc §13: escalation chain is configurable.
func (pam *PositionActionManager) EscalateAction(actionID uuid.UUID, deficit int64) (*PositionAction, error) {
	action, ok := pam.actions[actionID]
	if !ok {
		return nil, fmt.Errorf("unknown action_id: %s", actionID)
	}

	action.Deficit = deficit
	action.State = ActionStateDeficit

	// Determine next action type in escalation chain
	var nextType ActionType
	switch action.ActionType {
	case ActionTypeLiquidation:
		nextType = ActionTypeADL
	case ActionTypeADL:
		nextType = ActionTypeDeleverage
	default:
		return nil, fmt.Errorf("no escalation path from %s", action.ActionType)
	}

	action.State = ActionStateEscalated

	// Create new action for escalation
	pos := pam.positionMgr.GetPosition(action.UserID, action.MarketID)
	if pos == nil || pos.IsFlat() {
		return nil, fmt.Errorf("no position to escalate")
	}

	newAction := &PositionAction{
		ActionID:      uuid.New(),
		ActionType:    nextType,
		UserID:        action.UserID,
		MarketID:      action.MarketID,
		State:         ActionStateTriggered,
		InitialSize:   pos.Size,
		RemainingSize: pos.Size,
		Deficit:       deficit,
		ParentID:      &action.ActionID,
	}

	pam.actions[newAction.ActionID] = newAction
	return newAction, nil
}

// GetActiveAction returns the active action for a user/market pair.
func (pam *PositionActionManager) GetActiveAction(userID uuid.UUID, marketID string) *PositionAction {
	for _, action := range pam.actions {
		if action.UserID == userID && action.MarketID == marketID && !action.IsTerminal() {
			return action
		}
	}
	return nil
}

func (pam *PositionActionManager) hasActiveAction(userID uuid.UUID, marketID string) bool {
	return pam.GetActiveAction(userID, marketID) != nil
}

// CleanupTerminal removes terminal actions older than the given sequence.
func (pam *PositionActionManager) CleanupTerminal(beforeSequence int64) {
	for id, action := range pam.actions {
		if action.IsTerminal() && action.TriggeredAt < beforeSequence {
			delete(pam.actions, id)
		}
	}
}
